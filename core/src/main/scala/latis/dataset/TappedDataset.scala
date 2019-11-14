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
  metadata: Metadata,
  model: DataType,
  val data: SampledFunction,
  operations: Seq[UnaryOperation] = Seq.empty
) extends AbstractDataset(metadata, model, operations) {
  
  /**
   * Returns a copy of this Dataset with the given Operation 
   * appended to its sequence of operations.
   */
  def withOperation(operation: UnaryOperation): Dataset = 
    new TappedDataset(
      metadata, 
      model, 
      data, 
      operations :+ operation
    )
    
  /**
   * Applies the operations and returns a Stream of Samples.
   */
  def samples: Stream[IO, Sample] = {
    //TODO: compile/optimize the operations

    // Recursive function to apply operations to the data
    def applyOps(ops: Seq[UnaryOperation], mod: DataType, dat: SampledFunction): SampledFunction =
      ops.headOption match {
        case Some(op) =>
          val mod2 = op.applyToModel(mod)
          val dat2 = op.applyToData(dat, mod)
          applyOps(ops.tail, mod2, dat2)
        case None => dat
      }

    applyOps(operations, model, data).streamSamples
  }
}

