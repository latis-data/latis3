package latis.dataset

import cats.effect.IO
import cats.syntax.all._
import fs2.Stream

import latis.data.SampledFunction
import latis.data._
import latis.metadata.Metadata
import latis.model.DataType
import latis.ops.UnaryOperation
import latis.util.Identifier
import latis.util.LatisException

/**
 * Defines a Dataset with data represented as a SampledFunction.
 *
 * This is considered "tapped" as opposed to "adapted" which has
 * an Adapter awaiting instructions. Once an AdaptedDataset is
 * "tapped" (by invoking the Adapter's getData method with a URI
 * and Operations) it is ready to stream data and cannot be untapped
 * to receive further instructions.
 */
class TappedDataset(
  _metadata: Metadata,
  _model: DataType,
  _data: Data,
  operations: Seq[UnaryOperation] = Seq.empty
) extends AbstractDataset(_metadata, _model, operations) with Serializable {

  /**
   * Returns this Dataset's Data with operations applied.
   * This can be expensive. It is recommended to use samples
   * when it is time to consume the data.
   * If Data is really needed, consider unsafeForce to get a
   * MemoizedDataset whose data returns a MemoizedFunction.
   */
  def data: Data = applyOperations().fold(throw _, identity)

  /**
   * Returns a copy of this Dataset with the given Operation
   * appended to its sequence of operations.
   */
  def withOperation(operation: UnaryOperation): Dataset =
    new TappedDataset(
      _metadata,
      _model,
      _data,
      operations :+ operation
    )

  def rename(newId: Identifier): Dataset =
    new TappedDataset(
      _metadata + ("id" -> newId.asString),
      _model,
      _data,
      operations
    )

  /**
   * Applies the operations and returns a Stream of Samples.
   */
  def samples: Stream[IO, Sample] =
    applyOperations().fold(Stream.raiseError[IO](_), _.samples)

  /**
   * Applies the Operations to generate the new Data.
   */
  protected def applyOperations(): Either[LatisException, Data] = {
    //TODO: compile/optimize the operations

    // Defines a function to apply an Operation to Data.
    // The model (DataType) needs to ride along to provide context.
    val f: ((DataType, Data), UnaryOperation) => Either[LatisException, (DataType, Data)] = {
      case ((model: DataType, data: Data), op: UnaryOperation) => for {
        mod2 <- op.applyToModel(model)
        dat2 <- data.applyOperation(op, model)
      } yield (mod2, dat2)
    }

    // Apply the operations to the data
    operations.toList.foldM((_model, _data))(f).map(_._2)
  }

  /**
   * Transforms this TappedDataset into a MemoizedDataset.
   * Operations will be applied and the resulting samples
   * will be read into a MemoizedFunction.
   */
  def unsafeForce(): MemoizedDataset = new MemoizedDataset(
    metadata,  //from super with ops applied
    model,     //from super with ops applied
    applyOperations().fold(throw _, identity).asInstanceOf[SampledFunction].unsafeForce
  )

}
