package latis.dataset

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all._
import fs2.Stream

import latis.data.Data
import latis.data.Sample
import latis.data.SampledFunction
import latis.data.StreamFunction
import latis.metadata.Metadata
import latis.model.DataType
import latis.ops.Append
import latis.ops.BinaryOperation
import latis.ops.UnaryOperation
import latis.util.Identifier
import latis.util.LatisException

/**
 * Defines a Dataset with data provided via a list of Datasets to be appended.
 */
class CompositeDataset(
  _metadata: Metadata,
  datasets: NonEmptyList[Dataset],
  operations: Seq[UnaryOperation] = Seq.empty
) extends Dataset {

  val joinOperation: BinaryOperation = Append()

  // TODO: implement metadata appending
  override def metadata: Metadata = _metadata

  override def model: DataType = (for {
    model <- compositeModel
    opsModel <- operations.foldM(model)((mod, op) => op.applyToModel(mod))
  } yield opsModel).fold(throw _, identity)

  /**
   * Returns a lazy fs2.Stream of Samples.
   */
  def samples: Stream[IO, Sample] =
    applyOperations().fold(Stream.raiseError[IO](_), _.samples)

  /**
   * Returns a new Dataset with the given Operation *logically*
   * applied to this one.
   */
  def withOperation(op: UnaryOperation): Dataset =
    new CompositeDataset(
      _metadata,
      datasets,
      operations :+ op
    )

  def rename(newId: Identifier): Dataset =
    new CompositeDataset(
      _metadata + ("id" -> newId.asString),
      datasets,
      operations
    )

  /**
   * Applies the Operations to generate the new Data.
   */
  private def applyOperations(): Either[LatisException, Data] = for {
      model <- compositeModel
      data <- compositeData
      newData <- operations.foldM((model, data)) { case ((mod1, dat1), op) =>
        (op.applyToModel(mod1), op.applyToData(dat1, mod1)).mapN((_, _))
      }.map(_._2)
    } yield newData

  /**
   * Causes the data source to be read and released
   * and existing Operations to be applied.
   */
  def unsafeForce(): MemoizedDataset = new MemoizedDataset(
    metadata,  //from super with ops applied
    model,     //from super with ops applied
    applyOperations().fold(throw _, identity).asInstanceOf[SampledFunction].unsafeForce
  )

  private lazy val compositeData: Either[LatisException, Data] =
    datasets.tail.foldM {
      StreamFunction(datasets.head.samples): Data
    } { (data, ds) =>
      joinOperation.applyToData(data, StreamFunction(ds.samples))
    }

  private lazy val compositeModel: Either[LatisException, DataType] =
    datasets.tail.foldM(datasets.head.model){ (model, ds) =>
      joinOperation.applyToModel(model, ds.model)
    }

}
