package latis.output

import cats.effect.IO
import fs2.Stream
import io.circe.Json
import io.circe.syntax.*

import latis.dataset.*
import latis.ops.Uncurry

class JsonEncoder extends Encoder[IO, Json] {
  //TODO: use Scalar.formatValue so we can apply precision...

  /**
   * Encodes the Stream of Samples from the given Dataset as a Stream
   * of JSON arrays.
   */
  override def encode(dataset: Dataset): Stream[IO, Json] = {
    val uncurriedDataset = dataset.withOperation(Uncurry())
    // Encode each Sample as a String in the Stream
    uncurriedDataset.samples.map(_.asJson)
  }
}
