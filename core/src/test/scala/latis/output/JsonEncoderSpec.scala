package latis.output

import cats.effect.unsafe.implicits.global
import io.circe._
import io.circe.syntax._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.data._
import latis.dataset.Dataset
import latis.dsl.DatasetGenerator

class JsonEncoderSpec extends AnyFlatSpec {

  /** Instance of TextEncoder for testing.  */
  val enc = new JsonEncoder

  "A JSON encoder" should "encode a dataset to JSON" in {
    val ds: Dataset = DatasetGenerator("x -> (a: int, b: double, c: string)")
    val encodedList = enc.encode(ds).compile.toList.unsafeRunSync()

    val expected = List(
      Json.arr(0.asJson, 0.asJson, 0.0.asJson, "a".asJson),
      Json.arr(1.asJson, 1.asJson, 1.0.asJson, "b".asJson),
      Json.arr(2.asJson, 2.asJson, 2.0.asJson, "c".asJson),
    )

    encodedList should be(expected)
  }

  "A JSON encoder" should "encode a Sample to JSON" in {
    val sample = Sample(DomainData(0), RangeData(1, 1.1, "a")).asJson
    val expected = Json.arr(0.asJson, 1.asJson, 1.1.asJson, "a".asJson)

    sample should be(expected)
  }

  it should "encode a NaN as null" in {
    Double.NaN.asJson.toString() should be("null")
  }

  it should "encode NullData as null" in {
    val data: Data = NullData
    data.asJson.toString() should be("null")
  }
}
