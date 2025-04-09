package latis.output

import cats.effect.IO
import fs2.Stream
import io.circe.Json
import io.circe.syntax.*

import latis.dataset.*
import latis.ops.Uncurry
import latis.util.LatisException

class JsonEncoder extends Encoder[IO, Json] {
  //TODO: use Scalar.formatValue so we can apply precision...

  /**
   * Encodes the Stream of Samples from the given Dataset as a Stream
   * of JSON arrays.
   *
   * If the Dataset represents a simply nested Function, it will be flattened
   * with the Uncurry operation. A complex nested Function (e.g. Tuple
   * containing a Function) will throw a LatisException (until we improve our types).
   */
  override def encode(dataset: Dataset): Stream[IO, Json] = {

    if (dataset.model.isComplex)
      throw LatisException(s"JsonEncoder does not support complex model: ${dataset.model}")

    val flatDataset =
      if (dataset.model.isSimplyNested) dataset.withOperation(Uncurry())
      else dataset

    // Encode each Sample as a String in the Stream
    flatDataset.samples.map(_.asJson)
  }
}
