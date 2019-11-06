package latis.dataset

import latis.ops.UnaryOperation
import latis.metadata.Metadata
import latis.model.DataType
import latis.input.Adapter
import java.net.URI
import fs2.Stream
import cats.effect.IO
import latis.data.Sample

/**
 * Defines a Dataset with data provided via an Adapter.
 */
class AdaptedDataset(
  metadata: Metadata,
  model: DataType,
  val adapter: Adapter,
  val uri: URI,
  operations: Seq[UnaryOperation] = Seq.empty
) extends AbstractDataset(
  metadata, 
  model, 
  operations
) {
  
  /**
   * Returns a copy of this Dataset with the given Operation 
   * appended to its sequence of operations.
   */
  def withOperation(operation: UnaryOperation): Dataset = 
    new AdaptedDataset(
      metadata, 
      model,
      adapter: Adapter,
      uri: URI,
      operations :+ operation
    )

  /**
   * Invokes the Adapter to return data as a SampledFunction.
   * Note that this could still be lazy, wrapping a unreleased
   * resource.
   * Contrast to "unsafeForce".
   */
  def unadapt(): UnadaptedDataset = {
    // Separate leading operation that the adapter can handle
    // from the rest.
    val adapterOps = operations.takeWhile(adapter.canHandleOperation(_))
    val otherOps = operations.drop(adapterOps.length)
    
    //TODO: add prov for adapter handled ops
    
    // Apply the adapter handled operations to the model
    // since the Adapter can't.
    val model2 = adapterOps.foldLeft(model)((mod, op) => 
      op.applyToModel(mod))
      
    // Delegate to the Adapter to get the (potentially lazy) data.
    val data = adapter.getData(uri, adapterOps)
    
    // Construct the new Dataset
    new UnadaptedDataset(metadata, model2, data)
  }
  
  /**
   * Returns a Stream of Samples from this Dataset.
   */
  def samples: Stream[IO, Sample] =
    unadapt().samples
}
