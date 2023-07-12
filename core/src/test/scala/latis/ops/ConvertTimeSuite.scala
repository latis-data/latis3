package latis.ops

import munit.FunSuite

import latis.data.*
import latis.metadata.Metadata
import latis.model.*
import latis.time.Time
import latis.time.TimeScale

class ConvertTimeSuite extends FunSuite {

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

  private lazy val convertTime = TimeScale.fromExpression("weeks since 2020-01-08")
    .map(ConvertTime(_)).fold(fail("failed to construct operation", _), identity)

  test("update the metadata of the time variable") {
    convertTime.applyToModel(numericTime) match {
      case Right(t: Time) =>
        assertEquals(t.valueType, DoubleValueType)
        //Note TimeScale.toString uses default ISO format
        assertEquals(t.units, Some("weeks since 2020-01-08T00:00:00.000Z"))
      case _ => fail("incorrect model")
    }
  }

  test("convert a numeric time variable") {
    val f = convertTime.mapFunction(numericTime)
    val sample = Sample(DomainData(), RangeData(14))
    f(sample) match {
      case Sample(_, RangeData(Number(t))) =>
        assertEquals(t, 1.0)
      case _ => fail("incorrect sample")
    }
  }

  test("convert a text time variable") {
    val f = convertTime.mapFunction(textTime)
    val sample = Sample(DomainData(), RangeData("2020-01-15"))
    f(sample) match {
      case Sample(_, RangeData(Number(t))) =>
        assertEqualsDouble(t, 1.0, 1e-9)
      case _ => fail("incorrect sample")
    }
  }

  test("convert multiple time variables") {
    val model = Function.from(
      textTime,
      numericTime
    ).fold(fail("failed to construct model", _), identity)
    val f = convertTime.mapFunction(model)
    val sample = Sample(DomainData("2020-01-15"), RangeData(14))
    f(sample) match {
      case Sample(DomainData(Number(t1)), RangeData(Number(t2))) =>
        assertEqualsDouble(t1, 1.0, 1e-9)
        assertEquals(t2, 1.0)
      case _ => fail("incorrect sample")
    }
  }

}
