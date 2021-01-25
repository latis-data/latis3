package latis.dataset

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all._
import fs2.Stream

import latis.data.Sample
import latis.data.StreamFunction
import latis.metadata.Metadata
import latis.model.DataType
import latis.ops.BinaryOperation
import latis.ops.UnaryOperation
import latis.util.LatisException

/**
 * Defines a Dataset with data provided via a list of Datasets.
 */
class CompositeDataset(
  _metadata: Metadata,
  _model: DataType,
  datasets: NonEmptyList[Dataset],
  joinOperation: BinaryOperation,
  operations: Seq[UnaryOperation] = Seq.empty
) extends AbstractDataset(
  _metadata,
  _model,
  operations
) {

  /**
   * Returns a lazy fs2.Stream of Samples.
   */
  def samples: fs2.Stream[IO, Sample] =
    tap().fold(Stream.raiseError[IO](_), _.samples)

  /**
   * Returns a new Dataset with the given Operation *logically*
   * applied to this one.
   */
  def withOperation(op: UnaryOperation): Dataset =
    new CompositeDataset(
      _metadata,
      _model,
      datasets,
      joinOperation,
      operations :+ op
    )

  /**
   * Causes the data source to be read and released
   * and existing Operations to be applied.
   */
  def unsafeForce(): MemoizedDataset = tap().fold(throw _, identity).unsafeForce()

  /**
   * Invokes the joinOperation to return data as a SampledFunction.
   * Note that this could still be lazy, wrapping a unreleased
   * resource.
   * Contrast to "unsafeForce".
   */
  def tap(): Either[LatisException, TappedDataset] =
    // TODO: add provenance
    // TODO: handle metadata
    datasets.tail.foldM {
      val ds = datasets.head
      new TappedDataset(_metadata, ds.model, StreamFunction(ds.samples), operations)
    } { (ds1, ds2) =>
      // join the data
      val data = joinOperation.applyToData(
        StreamFunction(ds1.samples),
        StreamFunction(ds2.samples)
      )
      // join the models
      val model = joinOperation.applyToModel(
        ds1.model,
        ds2.model
      )
      // make a tapped dataset
      (data, model).mapN((d, m) => new TappedDataset(_metadata, m, d, operations))
    }
}
