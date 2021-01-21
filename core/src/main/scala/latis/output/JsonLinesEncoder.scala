package latis.output

import cats.effect.IO
import fs2.Stream
import io.circe.Json
import io.circe.syntax._

import latis.dataset._
import latis.ops.Uncurry

class JsonLinesEncoder extends Encoder[IO, Json] {

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
