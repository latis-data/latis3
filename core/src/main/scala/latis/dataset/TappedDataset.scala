package latis.dataset

import cats.effect.IO
import cats.syntax.all._
import fs2.Stream

import latis.data.SampledFunction
import latis.data._
import latis.metadata.Metadata
import latis.model.DataType
import latis.ops.UnaryOperation
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
   * Returns this Dataset's Data.
   */
  def data: Data = _data

  /**
   * Returns this Dataset's data as a SampledFunction.
   */
  def function: SampledFunction = _data.asFunction

  /**
   * Returns a copy of this Dataset with the given Operation
   * appended to its sequence of operations.
   */
  def withOperation(operation: UnaryOperation): Dataset =
    new TappedDataset(
      _metadata,
      _model,
      data,
      operations :+ operation
    )

  /**
   * Applies the operations and returns a Stream of Samples.
   */
  def samples: Stream[IO, Sample] =
    applyOperations().fold(Stream.raiseError[IO](_), _.samples)

  /**
   * Applies the Operations to the Data and returns a SampledFunction.
   */
  private def applyOperations(): Either[LatisException, SampledFunction] = {
    //TODO: compile/optimize the operations

    // Defines a function to apply an Operation to a SampledFunction.
    // The model (DataType) needs to ride along to provide context.
    val f: ((DataType, SampledFunction), UnaryOperation) => Either[LatisException, (DataType, SampledFunction)] = {
      case ((model: DataType, data: SampledFunction), op: UnaryOperation) =>
        val mod2 = op.applyToModel(model)
        // Enables a smart SampledFunction to apply the Operation.
        // Note that it will return a superclass if it can't.
        val dat2 = data.asFunction.applyOperation(op, model)
        mod2.product(dat2)
    }

    // Apply the operations to the data
    operations.toList.foldM((_model, data.asFunction))(f).map(_._2)
  }

  /**
   * Transforms this TappedDataset into a MemoizedDataset.
   * Operations will be applied and the resulting samples
   * will be read into a MemoizedFunction.
   */
  def unsafeForce(): MemoizedDataset = new MemoizedDataset(
    metadata,  //from super with ops applied
    model,     //from super with ops applied
    applyOperations().fold(throw _, identity).unsafeForce
  )

}
