package latis.time

import org.scalatest.EitherValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.Inside.inside

import latis.data.Data
import latis.metadata.Metadata
import latis.util.Identifier.IdentifierStringContext

class TimeSpec extends AnyFlatSpec {
  
  val formattedTime: Time = Time.fromMetadata(
    Metadata(
      "id"    -> "time",
      "type"  -> "string",
      "units" -> "MMM dd, yyyy"
    )
  ).value

  private val numericTime = Time.fromMetadata(
    Metadata(
      "id"    -> "time",
      "type"  -> "double",
      "units" -> "seconds since 2000-01-01"
    )
  ).value
      
  
  "A numeric time" should "convert a string to a number" in {
    //Note that this is interpreted as a number rather than ISO yyMMdd
    inside(numericTime.convertValue("864000")) {
      case Right(d: Data.DoubleValue) =>
        d.value should be (864000D)
    }
  }

  "A numeric time" should "convert an ISO time" in {
    //Note that this fails the numeric check then tries ISO
    inside(numericTime.convertValue("2000-001")) {
      case Right(d: Data.DoubleValue) =>
        d.value should be (0D)
    }
  }

  "A formatted time" should "convert a time with matching format" in {
    //Note that this uses the time's format rather than ISO
    inside(formattedTime.convertValue("Jan 01, 2000")) {
      case Right(d: Data.StringValue) =>
        d.value should be("Jan 01, 2000")
    }
  }

  "A formatted time" should "convert a numeric looking ISO time" in {
    //Note that this fails to match the current format then tries ISO.
    //Seven digits are interpreted as yyyyDDD.
    inside(formattedTime.convertValue("2000001")) {
      case Right(d: Data.StringValue) =>
        d.value should be("Jan 01, 2000")
    }
  }
  
  "Time ordering" should "compare two formatted time values" in {
    formattedTime.ordering.tryCompare("Jan 01, 2000", "Feb 01, 2000") should be (Some(-1))
  }

  "rename" should "preserve type" in {
    assert(numericTime.rename(id"bar").isInstanceOf[Time])
  }
}
