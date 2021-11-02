package latis.dataset

import java.net.URI

import scala.util.Success
import scala.util.Try

import cats.effect.IO
import fs2.Stream

import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.ops._
import latis.util.Identifier
import latis.util.dap2.parser.ast

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
 *
 * If a granule may contain multiple samples, then the granule list dataset
 * must have metadata to enable selections with bin semantics (e.g. a binWidth
 * defined on the domain variable).
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
      granuleList                           //start with granule list dataset
        .withOperations(listOps)            //add operations to granule list
        .samples                            //get samples, one for each granule
        .map(s => Try(granuleToDataset(s))) //convert each sample to a dataset
        .collect { case Success(ds) => ds } //drop broken granules
        .compile.toList                     //get the list of datasets from stream (in IO)
        .map(makeDataset)                   //combine the granules into a single dataset
        .map(_.withOperations(operations))  //add operations to dataset
    ).flatMap(_.samples)                    //eval back into stream then get samples

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
          // Get the domain variable id, account for potential rename
          val domainId = listOps.collect {
            case Rename(_, id) => id
          }.lastOption.getOrElse(s.id)
          (domainId == id) &&  //Selection target must match domain id
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
      // Bin semantics don't match partial bins with Gt or Lt but we need partial granules
      val pdop = op match {
        case Selection(id, ast.Lt, value) => Selection(id, ast.LtEq, value)
        case Selection(id, ast.Gt, value) => Selection(id, ast.GtEq, value)
        case _ => op
      }
      new GranuleAppendDataset(
        dsIdentifier,
        granuleList,
        granuleModel,
        granuleToDataset,
        listOps = listOps :+ pdop,
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

  def unsafeForce(): MemoizedDataset = ??? //to be deprecated
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

}
