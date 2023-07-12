package latis.time

import munit.FunSuite

import latis.data.Data
import latis.metadata.Metadata
import latis.util.Identifier.*

class TimeSuite extends FunSuite {

  val formattedTime: Time = Time.fromMetadata(
    Metadata(
      "id"    -> "time",
      "type"  -> "string",
      "units" -> "MMM dd, yyyy"
    )
  ).getOrElse(fail("failed to create Time"))

  private lazy val numericTime = Time.fromMetadata(
    Metadata(
      "id"    -> "time",
      "type"  -> "double",
      "units" -> "seconds since 2000-01-01"
    )
  ).getOrElse(fail("failed to create Time"))


  test("convert an ISO string to a number") {
    // Note that this is interpreted as ISO year 2000
    assertEquals(
      numericTime.convertValue("2000"),
      Right(Data.DoubleValue(0.0))
    )
  }

  test("convert a non-ISO string to a number") {
    // Note that this is interpreted as a number since it is not valid ISO
    assertEquals(
      numericTime.convertValue("12345"),
      Right(Data.DoubleValue(12345.0))
    )
  }

  test("convert formatted time to ISO time") {
    assertEquals(
      formattedTime.convertValue("2000001"),
      Right(Data.StringValue("Jan 01, 2000"))
    )
  }

  test("convert a non-ISO time with native format") {
    // Note that this uses the time's format since it is not valid ISO
    assertEquals(
      formattedTime.convertValue("Jan 01, 2000"),
      Right(Data.StringValue("Jan 01, 2000"))
    )
  }

  test("interpret ambiguously formatted time as ISO first") {
    val time: Time = Time.fromMetadata(
      Metadata(
        "id"    -> "time",
        "type"  -> "string",
        "units" -> "ddMMyyyy"
      )
    ).getOrElse(fail("failed to create Time"))

    assertEquals(
      time.convertValue("19990102"),
      Right(Data.StringValue("02011999"))
    )
  }

  test("compare two formatted time values") {
    assertEquals(formattedTime.ordering.tryCompare("Jan 01, 2000", "Feb 01, 2000"), Some(-1))
  }

  test("preserve type with rename") {
    assert(numericTime.rename(id"bar").isInstanceOf[Time])
  }
}
