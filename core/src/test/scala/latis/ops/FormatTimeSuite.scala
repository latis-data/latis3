package latis.ops

import munit.FunSuite

import latis.data.*
import latis.metadata.Metadata
import latis.model.*
import latis.time.Time
import latis.time.TimeFormat

class FormatTimeSuite extends FunSuite {

  private lazy val numericTime = Time.fromMetadata(
    Metadata(
      "id" -> "t1",
      "type" -> "int",
      "units" -> "days since 2020-01-01"
    )
  ).fold(fail("failed to construct time", _), identity)

  private lazy val textTime = Time.fromMetadata(
    Metadata(
      "id" -> "t2",
      "type" -> "string",
      "units" -> "yyyy-MM-dd"
    )
  ).fold(fail("failed to construct time", _), identity)

  private lazy val formatTime =
    TimeFormat.fromExpression("yyyy-DDD")
      .map(FormatTime(_))
      .fold(fail("failed to construct operation", _), identity)

  test("update the metadata of the time variable") {
    formatTime.applyToModel(numericTime) match {
      case Right(t: Time) =>
        assertEquals(t.valueType, StringValueType)
        assertEquals(t.units, Some("yyyy-DDD"))
      case _ => fail("")
    }
  }

  test("format a numeric time variable") {
    val f = formatTime.mapFunction(numericTime)
    val sample = Sample(DomainData(), RangeData(14))
    assertEquals(f(sample), Sample(List.empty, List("2020-015")))
  }

  test("format a text time variable") {
    val f = formatTime.mapFunction(textTime)
    val sample = Sample(DomainData(), RangeData("2020-01-15"))
    assertEquals(f(sample), Sample(List.empty, List("2020-015")))
  }

  test("convert multiple time variables") {
    val model = Function.from(
      textTime,
      numericTime
    ).fold(fail("failed to construct model", _), identity)
    val f = formatTime.mapFunction(model)
    val sample = Sample(DomainData("2020-01-15"), RangeData(14))
    assertEquals(f(sample), Sample(List("2020-015"), List("2020-015")))
  }

}
