package latis.output

import io.circe._
import io.circe.syntax._
import munit.CatsEffectSuite

import latis.data._
import latis.dataset.Dataset
import latis.dsl.DatasetGenerator

class JsonEncoderSuite extends CatsEffectSuite {

  /**)stance of TextEncoder for testing.  */
  val enc = new JsonEncoder

  test("encode a dataset to JSON") {
    val ds: Dataset = DatasetGenerator("x -> (a:int, b: double, c: string)")

    val expected = List(
      Json.arr(0.asJson, 0.asJson, 0.0.asJson, "a".asJson),
      Json.arr(1.asJson, 1.asJson, 1.0.asJson, "b".asJson),
      Json.arr(2.asJson, 2.asJson, 2.0.asJson, "c".asJson),
    )

    enc.encode(ds).compile.toList.assertEquals(expected)
  }

  test("encode a Sample to JSON") {
    val sample = Sample(DomainData(0), RangeData(1, 1.1, "a")).asJson
    val expected = Json.arr(0.asJson, 1.asJson, 1.1.asJson, "a".asJson)

    assertEquals(sample, expected)
  }

  test("encode a NaN as null") {
    assertEquals(Double.NaN.asJson.toString(), "null")
  }

  test("encode NullData as null") {
    val data: Data = NullData
    assertEquals(data.asJson.toString(), "null")
  }

  test("encode a true boolean") {
    val data: Data = Data.BooleanValue(true)
    assertEquals(data.asJson.toString(), "true")
  }

  test("encode a false boolean") {
    val data: Data = Data.BooleanValue(false)
    assertEquals(data.asJson.toString(), "false")
  }
}
