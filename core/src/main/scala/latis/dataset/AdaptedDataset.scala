package latis.dataset

import java.net.URI

import cats.effect.IO
import cats.syntax.all._
import fs2.Stream

import latis.data.Sample
import latis.input.Adapter
import latis.metadata.Metadata
import latis.model.DataType
import latis.ops.UnaryOperation
import latis.util.Identifier
import latis.util.LatisException

/**
 * Defines a Dataset with data provided via an Adapter.
 */
class AdaptedDataset(
  _metadata: Metadata,
  _model: DataType,
  adapter: Adapter,
  uri: URI,
  operations: Seq[UnaryOperation] = Seq.empty
) extends AbstractDataset(
  _metadata,
  _model,
  operations
) {

  /**
   * Returns a copy of this Dataset with the given Operation
   * appended to its sequence of operations.
   */
  def withOperation(operation: UnaryOperation): Dataset =
    new AdaptedDataset(
      _metadata,
      _model,
      adapter: Adapter,
      uri: URI,
      operations :+ operation
    )

  def rename(newId: Identifier): Dataset =
    new AdaptedDataset(
      _metadata + ("id" -> newId.asString),
      _model,
      adapter: Adapter,
      uri: URI,
      operations
    )

  /**
   * Invokes the Adapter to return data as a SampledFunction.
   * Note that this could still be lazy, wrapping a unreleased
   * resource.
   * Contrast to "unsafeForce".
   */
  def tap(): Either[LatisException, TappedDataset] = {
    // Separate leading operation that the adapter can handle
    // from the rest. Note that we must preserve the order for safety.
    //TODO: "compile" the Operations to optimize the order of application
    val adapterOps = operations.takeWhile(adapter.canHandleOperation(_))
    val otherOps   = operations.drop(adapterOps.length)

    //TODO: add prov for adapter handled ops

    // Apply the adapter handled operations to the model
    // since the Adapter can't.
    val model2 = adapterOps.toList
      .foldM(_model)((mod, op) => op.applyToModel(mod))

    // Delegate to the Adapter to get the (potentially lazy) data.
    val data = adapter.getData(uri, adapterOps)

    // Construct the new Dataset
    model2.map(new TappedDataset(_metadata, _, data, otherOps))
  }

  /**
   * Returns a Stream of Samples from this Dataset.
   */
  def samples: Stream[IO, Sample] = tap().fold(Stream.raiseError[IO](_), _.samples)

  /**
   * Transforms this TappedDataset into a MemoizedDataset.
   * Operations will be applied and the resulting samples
   * will be read into a MemoizedFunction.
   */
  def unsafeForce(): MemoizedDataset = tap().fold(throw _, identity).unsafeForce()
}
