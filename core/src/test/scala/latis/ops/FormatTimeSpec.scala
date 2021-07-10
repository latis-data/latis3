package latis.ops

import org.scalatest.EitherValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.Inside.inside

import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.time.Time
import latis.time.TimeFormat

class FormatTimeSpec extends AnyFlatSpec {

  private val numericTime = Time.fromMetadata(
    Metadata(
      "id" -> "t1",
      "type" -> "int",
      "units" -> "days since 2020-01-01"
    )
  ).value

  private val textTime = Time.fromMetadata(
    Metadata(
      "id" -> "t2",
      "type" -> "string",
      "units" -> "yyyy-MM-dd"
    )
  ).value

  private val formatTime = FormatTime(TimeFormat.fromExpression("yyyy-DDD").value)

  "The FormatTime Operation" should "update the metadata of the time variable" in {
    inside(formatTime.applyToModel(numericTime)) {
      case Right(t: Time) =>
        t.valueType should be(StringValueType)
        t.units should be(Some("yyyy-DDD"))
    }
  }

  it should "format a numeric time variable" in {
    val f = formatTime.mapFunction(numericTime)
    val sample = Sample(DomainData(), RangeData(14))
    inside(f(sample)) {
      case Sample(_, RangeData(Text(t))) =>
        t should be("2020-015")
    }
  }

  it should "format a text time variable" in {
    val f = formatTime.mapFunction(textTime)
    val sample = Sample(DomainData(), RangeData("2020-01-15"))
    inside(f(sample)) {
      case Sample(_, RangeData(Text(t))) =>
        t should be("2020-015")
    }
  }

  it should "convert multiple time variables" in {
    val model = Function.from(
      textTime,
      numericTime
    ).value
    val f = formatTime.mapFunction(model)
    val sample = Sample(DomainData("2020-01-15"), RangeData(14))
    inside(f(sample)) {
      case Sample(DomainData(Text(t1)), RangeData(Text(t2))) =>
        t1 should be("2020-015")
        t2 should be("2020-015")
    }
  }

}
