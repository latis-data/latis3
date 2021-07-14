package latis.ops

import org.scalatest.EitherValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.Inside.inside

import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.time.Time
import latis.time.TimeScale

class ConvertTimeSpec extends AnyFlatSpec {

  private lazy val numericTime = Time.fromMetadata(
    Metadata(
      "id" -> "t1",
      "type" -> "int",
      "units" -> "days since 2020-01-01"
    )
  ).value

  private lazy val textTime = Time.fromMetadata(
    Metadata(
      "id" -> "t2",
      "type" -> "string",
      "units" -> "yyyy-MM-dd"
    )
  ).value

  private lazy val convertTime = TimeScale.fromExpression("weeks since 2020-01-08")
    .map(ConvertTime(_)).value

  "The ConvertTime Operation" should "update the metadata of the time variable" in {
    inside(convertTime.applyToModel(numericTime)) {
      case Right(t: Time) =>
        t.valueType should be(DoubleValueType)
        //Note TimeScale.toString uses default ISO format
        t.units should be(Some("weeks since 2020-01-08T00:00:00.000Z"))
    }
  }

  it should "convert a numeric time variable" in {
    val f = convertTime.mapFunction(numericTime)
    val sample = Sample(DomainData(), RangeData(14))
    inside(f(sample)) {
      case Sample(_, RangeData(Number(t))) =>
        t should be(1.0)
    }
  }

  it should "convert a text time variable" in {
    val f = convertTime.mapFunction(textTime)
    val sample = Sample(DomainData(), RangeData("2020-01-15"))
    inside(f(sample)) {
      case Sample(_, RangeData(Number(t))) =>
        t should be(1.0 +- 1e-9)
    }
  }

  it should "convert multiple time variables" in {
    val model = Function.from(
      textTime,
      numericTime
    ).value
    val f = convertTime.mapFunction(model)
    val sample = Sample(DomainData("2020-01-15"), RangeData(14))
    inside(f(sample)) {
      case Sample(DomainData(Number(t1)), RangeData(Number(t2))) =>
        t1 should be(1.0 +- 1e-9)
        t2 should be(1.0)
    }
  }

}
