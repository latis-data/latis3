package latis.dataset

import cats.effect.IO
import fs2.Stream

import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.ops._
import latis.util.Identifier

/**
 * A GranuleAppendDataset combines individual (granule) Datasets
 * as defined by a granuleList Dataset.
 *
 * The granule list Dataset is expected to define granules that are ordered
 * with no overlap of coverage. Each granule is expected to have the same model.
 * As such, the Samples from each granule can be appended while preserving
 * order without duplication.
 *
 * The general use case expects that the granule list Dataset has a single
 * domain variable (usually time) that is the same as the first dimension of
 * the granule datasets. Selections on that common variable can often be pushed down
 * to the granule list to limit the number of granules that need to be accessed.
 * In such cases, the granule list selection is treated with contiguous bin
 * semantics such that the value of the domain variable of the granule list
 * represents the inclusive start of the bin which ends with the exclusive
 * start of the next granule. This assumes that the domain value for the granule
 * in the granule list is less than or equal to the domain value of the first
 * sample in the granule. For example, daily files (i.e. granules) could be
 * represented as a time series of URLs (i.e. granule list dataset): time -> url
 * with time values for the start of the day. Each file might contain hourly samples
 * for that day that we want to combine with samples from other files.
 *
 * The granule list may have a different domain than the granules as long as
 * uniqueness and ordering is maintained when simply appending Samples from the
 * granules. No push-down optimization will be applied to such a granule list.
 *
 * Each Sample of the granule list Dataset becomes a granule Dataset via the
 * granuleToDataset function. These Datasets are combined as a CompositeDataset
 * using the Append join operation. This assumes that the granules are aligned
 * in the proper order with no overlap in coverage. The CompositeDataset may
 * enable operation push-down to the granules.
 *
 * The opportunity to push down an operation is limited by the commutative
 * properties of operations that precede it.
 */
class GranuleAppendDataset private (
  dsIdentifier: Identifier,
  granuleList: Dataset,
  granuleModel: DataType,
  granuleToDataset: Sample => Dataset,
  listOps: List[UnaryOperation] = List.empty,
  operations: List[UnaryOperation] = List.empty
) extends Dataset {
  //TODO: support nD tiles

  /*
  TODO: apply bin semantics
    require cadence?

  TODO: handle error in granuleToDataset
    how much can be raised into stream's IO error channel?
    what if granule sample is invalid?
    should function return Either?
    just deal with thrown exceptions?

  TODO: migrate sorted join from packets
  TODO: migrate type matcher from packets
   */

  def metadata: Metadata = Metadata(dsIdentifier) //TODO: add prov, see AbstractDataset

  def model: DataType = operations.foldLeft(granuleModel)((mod, op) => op.applyToModel(mod).fold(throw _, identity))

  /**
   * Combines a List of granule Datasets into a single Dataset.
   */
  private def makeDataset(granules: List[Dataset]): Dataset = granules match {
    case ds1 :: ds2 :: dss => CompositeDataset(dsIdentifier, Append(), ds1, ds2, dss)
    case ds :: Nil         => ds //just one granule, TODO: rename it with dsIdentifier?
    case Nil               => new TappedDataset(metadata, granuleModel, SeqFunction(Seq.empty)) //empty dataset
  }

  /**
   * Constructs a Stream of Samples from the granule Datasets with operations applied.
   */
  def samples: Stream[IO, Sample] =
    Stream.eval(
      granuleList                          //start with granule list dataset
        .withOperations(listOps)           //add operations to granule list
        .samples                           //get samples, one for each granule
        .map(granuleToDataset)             //convert each sample to a dataset
        .compile.toList                    //get the list of datasets from stream (in IO)
        .map(makeDataset)                  //combine the granules into a single dataset
        .map(_.withOperations(operations)) //add operations to dataset
    ).flatMap(_.samples)                   //eval back into stream then get samples

  /** Returns the granule list domain Scalar if it is one-dimensional. */
  private def granuleDomain: Option[Scalar] = granuleList.model match {
    case Function(s: Scalar, _) => Some(s)
    case _ => None
  }

  /**
   * Indicates if the given operation can be applied to the granule list dataset.
   */
  private def canPushDown(op: UnaryOperation): Boolean = op match {
    case Selection(id, _, _) =>
      // Push down selections that target the granule list domain variable.
      // Prevent application to the list if certain operations have already been added.
      granuleDomain match {
        case Some(s: Scalar) =>
          (s.id == id) && //TODO: must rewrite to match last rename
          operations.forall {
            // Can we safely push down the selection to the granule list dataset
            // if the given operation has already been added.
            // Note that this does NOT reorder the operations.
            case _: Filter     => true
            case _: Projection => true
            case _: Taking     => true
            case _: Rename     => true
            case _ => false
          }
        case _ => false //not a 1D Function
      }
    case Rename(rid, _) =>
      // Push down Rename if it targets the granule list domain.
      // Account for previous renames by using the latest.
      // This allows us to safely push down selections that follow a rename.
      // Note that this assumes only one domain variable.
      // TODO: drop this condition if rename with a missing target is a no-op?
      listOps.collect {
        case Rename(_, id) => id
      }.lastOption.fold(granuleDomain.map(_.id).contains(rid))(_ == rid)
    case _ => false
  }

  /**
   * Makes a copy of this Dataset with the given operation added to the list
   * to be applied. Some operations may also be applied (pushed down) to the
   * granule list Dataset for optimization purposes.
   */
  def withOperation(op: UnaryOperation): Dataset = {
    if (canPushDown(op)) {
      new GranuleAppendDataset(
        dsIdentifier,
        granuleList,
        granuleModel,
        granuleToDataset,
        listOps = listOps :+ op,
        operations = operations :+ op
      )
    } else {
      new GranuleAppendDataset(
        dsIdentifier,
        granuleList,
        granuleModel,
        granuleToDataset,
        listOps = listOps,
        operations = operations :+ op
      )
    }
  }

  /*
  TODO: nearest neighbor
    Note: should not be handled as a Selection, not a filter
    e.g. last second of minute cadence in daily files
      the time nominally falls within the bounds of the file but the nearest is in the next file
      use time >= n & take(2)?
      consider also start/previous
        would need to know cadence of files?
        can we use a 3 element sliding window?
        or just best effort?
   */

  def unsafeForce(): MemoizedDataset = ???
}

object GranuleAppendDataset {

  def apply(
    id: Identifier,
    granuleList: Dataset,
    model: DataType,
    granuleToDataset: Sample => Dataset
  ): GranuleAppendDataset = new GranuleAppendDataset(
    id,
    granuleList,
    model,
    granuleToDataset
  )

  /*
  TODO: FDML support
    Assign "class" in "dataset" element, default to AdaptedDataset, use GranuleAppendDataset here
    source is granuleList dataset (preferably via ref)
    adapter and model are for granule
    construct GranuleAppendDataset
      make adapter from AdapterConfig
      granuleToDataset: extract uri from granuleList, invoke adapter
        require "uri" variable or var of type URI (subclass of scalar)?
   */
//  def apply(
//    id: Identifier,
//    granuleList: Dataset,
//    model: DataType,
//    adapterConfig: AdapterConfig
//  ): GranuleAppendDataset = ???

}
