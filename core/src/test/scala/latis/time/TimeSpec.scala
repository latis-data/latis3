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

  private lazy val numericTime = Time.fromMetadata(
    Metadata(
      "id"    -> "time",
      "type"  -> "double",
      "units" -> "seconds since 2000-01-01"
    )
  ).value


  "A numeric time" should "convert an ISO string to a number" in {
    // Note that this is interpreted as ISO year 2000
    inside(numericTime.convertValue("2000")) {
      case Right(d: Data.DoubleValue) =>
        d.value should be (0)
    }
  }

  it should "convert a non-ISO string to a number" in {
    // Note that this is interpreted as a number since it is not valid ISO
    inside(numericTime.convertValue("12345")) {
      case Right(d: Data.DoubleValue) =>
        d.value should be (12345)
    }
  }

  "A formatted time" should "convert an ISO time" in {
    inside(formattedTime.convertValue("2000001")) {
      case Right(d: Data.StringValue) =>
        d.value should be("Jan 01, 2000")
    }
  }

  it should "convert a non-ISO time with native format" in {
    // Note that this uses the time's format since it is not valid ISO
    inside(formattedTime.convertValue("Jan 01, 2000")) {
      case Right(d: Data.StringValue) =>
        d.value should be("Jan 01, 2000")
    }
  }

  "An ambiguously formatted time" should "be interpreted as ISO first" in {
    val time: Time = Time.fromMetadata(
      Metadata(
        "id"    -> "time",
        "type"  -> "string",
        "units" -> "ddMMyyyy"
      )
    ).value
    inside(time.convertValue("19990102")) {
      case Right(d: Data.StringValue) =>
        d.value should be("02011999")
    }
  }

  
  "Time ordering" should "compare two formatted time values" in {
    formattedTime.ordering.tryCompare("Jan 01, 2000", "Feb 01, 2000") should be (Some(-1))
  }

  "rename" should "preserve type" in {
    assert(numericTime.rename(id"bar").isInstanceOf[Time])
  }
}
