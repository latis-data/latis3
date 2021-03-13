package latis.ops

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.time.Time
import latis.time.TimeFormat

class FormatTimeSpec extends AnyFlatSpec {

  val numericTime = Time(
    Metadata(
      "id" -> "t1",
      "type" -> "int",
      "units" -> "days since 2020-01-01",
      "class" -> "latis.time.Time"
    )
  )

  val textTime = Time(
    Metadata(
      "id" -> "t2",
      "type" -> "string",
      "units" -> "yyyy-MM-dd",
      "class" -> "latis.time.Time"
    )
  )

  val formatTime = FormatTime(TimeFormat("yyyy-DDD"))

  "The FormatTime Operation" should "update the metadata of the time variable" in {
    val newModel = formatTime.applyToModel(numericTime).toTry.get
    //println(newModel.metadata.properties)
    newModel("type") should be(Some("string"))
    newModel("units") should be(Some("yyyy-DDD"))
  }

  it should "format a numeric time variable" in {
    val f = formatTime.mapFunction(numericTime)
    val sample = Sample(DomainData(), RangeData(14))
    f(sample) match {
      case Sample(_, RangeData(Text(t))) =>
        t should be("2020-015")
    }
  }

  it should "format a text time variable" in {
    val f = formatTime.mapFunction(textTime)
    val sample = Sample(DomainData(), RangeData("2020-01-15"))
    f(sample) match {
      case Sample(_, RangeData(Text(t))) =>
        t should be("2020-015")
    }
  }

  it should "convert multiple time variables" in {
    val model = Function(
      textTime,
      numericTime
    )
    val f = formatTime.mapFunction(model)
    val sample = Sample(DomainData("2020-01-15"), RangeData(14))
    f(sample) match {
      case Sample(DomainData(Text(t1)), RangeData(Text(t2))) =>
        t1 should be("2020-015")
        t2 should be("2020-015")
    }
  }

}
