package latis.output

import io.circe._
import io.circe.syntax._

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data.Sample
import latis.data.RangeData
import latis.data.DomainData
import latis.dataset.Dataset
import latis.util.Identifier.IdentifierStringContext

class JsonLinesEncoderSpec extends FlatSpec {

  /**
   * Instance of TextEncoder for testing.
   */
  val enc = new JsonLinesEncoder
  val ds: Dataset = Dataset.fromName(id"data")

  "A JSON Lines encoder" should "encode a dataset to JSON Lines" in {
    val encodedList = enc.encode(ds).compile.toList.unsafeRunSync()

    val expected = List(
      Json.arr(0.asJson, 1.asJson, 1.1.asJson, "a".asJson),
      Json.arr(1.asJson, 2.asJson, 2.2.asJson, "b".asJson),
      Json.arr(2.asJson, 4.asJson, 3.3.asJson, "c".asJson),
    )

    encodedList should be(expected)
  }

  "A JSON Lines encoder" should "encode a Sample to JSON Lines" in {
    val sample = Sample(DomainData(0), RangeData(1, 1.1, "a")).asJson
    val expected = Json.arr(0.asJson, 1.asJson, 1.1.asJson, "a".asJson)

    sample should be(expected)
  }
}
