package latis.dataset

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream

import latis.data.*
import latis.metadata.Metadata
import latis.model.DataType
import latis.ops.*
import latis.util.LatisException

/**
 * Defines a Dataset with data provided via a list of Datasets to be
 * combined with the given join operation.
 *
 * This assumes that each dataset has the same model, for now.
 */
class CompositeDataset private (
  md: Metadata,
  datasets: NonEmptyList[Dataset],
  joinOperation: Join,
  granuleOps: List[UnaryOperation] = List.empty, //ops to be applied to granules before the join
  afterOps: List[UnaryOperation] = List.empty    //ops to be applied after the join
) extends Dataset {
  //TODO: support "horizontal" joins: datasets with different models (i.e. diff set of variables, combine "columns")

  //TODO: make richer metadata, prov
  override def metadata: Metadata = md

  def operations: List[UnaryOperation] = granuleOps ++ afterOps

  /**
   * Returns a new Dataset with the given Operation *logically*
   * applied to this one.
   */
  def withOperation(op: UnaryOperation): Dataset = {
    if (nonReappliedAfterOps.isEmpty) op match {
      //TODO: push down other operations?
      //TODO: make sure granule (after current granuleOps application) has target variable? or no-op?
        //matters for joins with different models, not Append or Sorted
      case _: Filter       => copyWithOperationForGranule(op)
      case _: MapOperation => copyWithOperationForGranule(op)
      case _: Rename       => copyWithOperationForGranule(op)
      case _: Taking       => copyWithOperationForBoth(op)
      case _               => copyWithOperationAfterJoin(op)
    }
    else copyWithOperationAfterJoin(op) //can't safely add this op before other afterOps
  }

  private def copyWithOperationForGranule(op: UnaryOperation): CompositeDataset =
    new CompositeDataset(md, datasets, joinOperation,
      granuleOps :+ op,
      afterOps
    )

  private def copyWithOperationAfterJoin(op: UnaryOperation): CompositeDataset =
    new CompositeDataset(md, datasets, joinOperation,
      granuleOps,
      afterOps :+ op
    )

  private def copyWithOperationForBoth(op: UnaryOperation): CompositeDataset =
    new CompositeDataset(md, datasets, joinOperation,
      granuleOps :+ op,
      afterOps :+ op
    )

  /**
   * Defines the list of operations to be applied after the join
   * that haven't already been applied to the granules.
   */
  private def nonReappliedAfterOps: List[UnaryOperation] = afterOps.filterNot(_.isInstanceOf[Taking])

  /**
   * Computes the model with the operations and join applied.
   * Assumes join does not affect the model, for now.
   */
  override lazy val model: DataType =
    (granuleOps ++ nonReappliedAfterOps).foldM(datasets.head.model) {
      (mod, op) => op.applyToModel(mod)
    }.fold(throw _, identity)

  /**
   * Return a Dataset's Stream of Samples.
   *
   * If there is an error in the Dataset Stream, an error message will be printed
   * and the resulting Stream will be empty.
   */
  private def getSamples(ds: Dataset): Stream[IO, Sample] = {
    ds.samples.handleErrorWith { error =>
      Stream.eval(IO.println(s"Failed to stream dataset: $error")).drain //TODO: debug log
    }
  }

  /**
   * Applies the Operations to generate the new Data.
   */
  private def applyOperations(): Either[LatisException, Data] = for {
    // Apply granule operations
    dss     <- datasets.map(_.withOperations(granuleOps)).asRight
    // Apply join
    data    <- dss.tail.foldM(StreamFunction(getSamples(dss.head)): Data) { //compiler needs type hint
      (dat, ds) => joinOperation.applyToData(dat, StreamFunction(getSamples(ds)))
    }
    // Apply other operations
    newData <- afterOps.foldM((dss.head.model, data)) {
      case ((mod, dat), op) =>
        (op.applyToModel(mod), op.applyToData(dat, mod)).mapN((_, _))
    }.map(_._2) //drop model, keep data
  } yield newData

  /**
   * Applies the operations and returns a Stream of Samples.
   */
  def samples: Stream[IO, Sample] =
    applyOperations().fold(Stream.raiseError[IO](_), _.samples)

  /**
   * Causes the data source to be read and released
   * and existing Operations to be applied.
   */
  def unsafeForce(): MemoizedDataset = new MemoizedDataset(
    metadata,
    model,
    applyOperations().fold(throw _, identity).asInstanceOf[SampledFunction].unsafeForce
  )

}


object CompositeDataset {

  /**
   * Constructs a CompositeDataset with the given identifier and join operation
   * to be applied to at least two datasets.
   */
  def apply(
    md: Metadata,
    joinOperation: Join,
    ds1: Dataset,
    ds2: Dataset,
    rest: List[Dataset] = List.empty
  ): CompositeDataset =
    new CompositeDataset(md, NonEmptyList.of(ds1, (ds2 :: rest) *), joinOperation)

}
