package latis.time

import munit.FunSuite

import latis.data.Data
import latis.metadata.Metadata
import latis.util.Identifier.IdentifierStringContext

class TimeSuite extends FunSuite {

  val formattedTime: Time = Time.fromMetadata(
    Metadata(
      "id"    -> "time",
      "type"  -> "string",
      "units" -> "MMM dd, yyyy"
    )
  ).getOrElse(fail("time not generated"))

  private lazy val numericTime = Time.fromMetadata(
    Metadata(
      "id"    -> "time",
      "type"  -> "double",
      "units" -> "seconds since 2000-01-01"
    )
  ).getOrElse(fail("time not generated"))


  test("convert an ISO string to a number") {
    // Note that this is interpreted as ISO year 2000
    numericTime.convertValue("2000") match {
      case Right(d: Data.DoubleValue) =>
        assertEquals(d.value, 0.0)
      case _ => fail("converted time not generated")
    }
  }

  test("convert a non-ISO string to a number") {
    // Note that this is interpreted as a number since it is not valid ISO
    numericTime.convertValue("12345") match {
      case Right(d: Data.DoubleValue) =>
        assertEquals(d.value, 12345.0)
      case _ => fail("converted time not generated")
    }
  }

  test("convert formatted time to ISO time") {
    formattedTime.convertValue("2000001") match {
      case Right(d: Data.StringValue) =>
        assertEquals(d.value, "Jan 01, 2000")
      case _ => fail("converted time not generated")
    }
  }

  test("convert a non-ISO time with native format") {
    // Note that this uses the time's format since it is not valid ISO
    formattedTime.convertValue("Jan 01, 2000") match {
      case Right(d: Data.StringValue) =>
        assertEquals(d.value, "Jan 01, 2000")
      case _ => fail("converted time not generated")
    }
  }

  test("interpret ambiguously formatted time as ISO first") {
    val time: Time = Time.fromMetadata(
      Metadata(
        "id"    -> "time",
        "type"  -> "string",
        "units" -> "ddMMyyyy"
      )
    ).getOrElse(fail("time not generated"))
    time.convertValue("19990102") match {
      case Right(d: Data.StringValue) =>
        assertEquals(d.value, "02011999")
      case _ => fail("converted time not generated")
    }
  }

  
  test("compare two formatted time values") {
    assertEquals(formattedTime.ordering.tryCompare("Jan 01, 2000", "Feb 01, 2000"), Some(-1))
  }

  test("preserve type with rename") {
    assert(numericTime.rename(id"bar").isInstanceOf[Time])
  }
}
