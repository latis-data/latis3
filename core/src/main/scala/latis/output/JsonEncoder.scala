package latis.output

import cats.effect.IO
import fs2.Stream
import io.circe.{Encoder => CEncoder}
import io.circe.Json
import io.circe.syntax._

import latis.data._
import latis.data.Data._
import latis.dataset._
import latis.ops.Uncurry

class JsonEncoder extends Encoder[IO, Json] {

  /**
   * Encodes the Stream of Samples from the given Dataset as a Stream
   * of JSON arrays.
   */
  override def encode(dataset: Dataset): Stream[IO, Json] = {
    val uncurriedDataset = dataset.withOperation(Uncurry())
    // Encode each Sample as a String in the Stream
    uncurriedDataset.samples.map(_.asJson)
  }

  /** Instance of io.circe.Encoder for Sample. */
  implicit val encodeSample: CEncoder[Sample] = new CEncoder[Sample] {
    final def apply(s: Sample): Json = s match {
      case Sample(ds, rs) => (ds ++ rs).asJson
    }
  }

  /** Instance of io.circe.Encoder for Data. */
  implicit val encodeData: CEncoder[Data] = new CEncoder[Data] {
    final def apply(value: Data): Json = value match {
      case x: ShortValue  => x.value.asJson
      case x: IntValue    => x.value.asJson
      case x: LongValue   => x.value.asJson
      case x: FloatValue  => x.value.asJson
      case x: DoubleValue => x.value.asJson
      case x: StringValue => x.value.asJson
      case x              => x.toString.asJson
    }
  }

}
