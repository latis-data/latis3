package latis.dataset

import latis.ops.UnaryOperation
import latis.metadata.Metadata
import latis.model.DataType
import latis.input.Adapter
import java.net.URI
import fs2.Stream
import cats.effect.IO
import latis.data.Sample
import latis.data.SampledFunction

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
  _data: SampledFunction,
  operations: Seq[UnaryOperation] = Seq.empty
) extends AbstractDataset(_metadata, _model, operations) {

  /**
   * Returns the data as a SampledFunction.
   */
  def data: SampledFunction = _data

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
  def samples: Stream[IO, Sample] = applyOperations().streamSamples
  
  /**
   * Applies the Operations and returns a SampledFunction.
   */
  private def applyOperations(): SampledFunction = {
    //TODO: compile/optimize the operations
    
    // Defines a function to apply an Operation to a SampledFunction.
    // The model (DataType) need to ride along to provide context.
    val f: ((DataType, SampledFunction), UnaryOperation) => (DataType, SampledFunction) =
      (modat: (DataType, SampledFunction), op: UnaryOperation) => modat match {
        case (model: DataType, data: SampledFunction) =>
          val mod2 = op.applyToModel(model)
          val dat2 = op.applyToData(data, model)
          (mod2, dat2)
      }
        
    // Apply the operations to the data
    operations.foldLeft(_model, data)(f)._2
  }
  
  /**
   * Transforms this TappedDataset into a MemoizedDataset.
   * Operations will be applied and the resulting samples
   * will be read into a MemoizedFunction.
   */
  def unsafeForce(): MemoizedDataset = new MemoizedDataset(
    metadata,  //from super with ops applied
    model,     //from super with ops applied
    applyOperations().unsafeForce
  )

}

