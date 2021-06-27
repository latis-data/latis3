package latis.ops

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.Inside.inside

import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.time.Time
import latis.time.TimeScale

class ConvertTimeSpec extends AnyFlatSpec {

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

  val convertTime = TimeScale.fromExpression("weeks since 2020-01-08")
    .map(ConvertTime(_)).toTry.get

  "The ConvertTime Operation" should "update the metadata of the time variable" in {
    val newModel = convertTime.applyToModel(numericTime).toTry.get
    //println(newModel.metadata.properties)
    newModel("type") should be(Some("double"))
    //Note TimeScale.toString uses default ISO format
    newModel("units") should be(Some("weeks since 2020-01-08T00:00:00.000Z"))
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
    val model = Function(
      textTime,
      numericTime
    )
    val f = convertTime.mapFunction(model)
    val sample = Sample(DomainData("2020-01-15"), RangeData(14))
    inside(f(sample)) {
      case Sample(DomainData(Number(t1)), RangeData(Number(t2))) =>
        t1 should be(1.0 +- 1e-9)
        t2 should be(1.0)
    }
  }

}
