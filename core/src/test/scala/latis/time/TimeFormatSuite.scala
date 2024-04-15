package latis.time

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.format.ResolverStyle

import munit.FunSuite

class TimeFormatSuite extends FunSuite {

  test("format to default ISO string") {
    val time = 1000L * (86400 + 3600 + 61)
    assertEquals(TimeFormat.formatIso(time), "1970-01-02T01:01:01.000Z")
  }

  test("parse a formatted time string") {
    assertEquals(
      TimeFormat.fromExpression("yyyyDDD").flatMap(_.parse("1970002")),
      Right(86400000L)
    )
  }

  test("parse a formatted time string with literals") {
    assertEquals(
      TimeFormat.fromExpression("yyyy'T'HH'Z'").flatMap( _.parse("1970T00Z")),
      Right(0L)
    )
  }

  test("format a time value") {
    assertEquals(
      TimeFormat.fromExpression("yyyy MMM dd HH:mm").map(_.format(3600000)),
      Right("1970 Jan 01 01:00")
    )
  }

  test("use a specified century for 2-digit years".ignore) {
//    val time: Long =  TimeFormat.fromExpression("yyMMdd")
//      .flatMap(_.setCenturyStart("3000").parse("330501")).getOrElse(0L)
//    TimeFormat.fromExpression("yyyy-MM-dd").map(_.format(time)) should be Right("3033-05-01"))
  }

  test("string representation") {
    assertEquals(TimeFormat.Iso.toString, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  }

  test("make TimeFormat from ISO date") {
    assertEquals(
      TimeFormat.fromIsoValue("1970-01-01").map(_.toString),
      Right("yyyy-MM-dd")
    )
    assertEquals(
      TimeFormat.fromIsoValue("19700101").map(_.toString),
      Right("yyyyMMdd")
    )
    assertEquals(
      TimeFormat.fromIsoValue("1970001").map(_.toString),
      Right("yyyyDDD")
    )
    assertEquals(
      TimeFormat.fromIsoValue("1970-001").map(_.toString),
      Right("yyyy-DDD")
    )
    assertEquals(
      TimeFormat.fromIsoValue("1970-01").map(_.toString),
      Right("yyyy-MM")
    )
    assertEquals(
      TimeFormat.fromIsoValue("1970").map(_.toString),
      Right("yyyy")
    )
  }

  test("make TimeFormat from ISO time") {
    assertEquals(
      TimeFormat.fromIsoValue("1970T00:00:00.000Z").map(_.toString),
      Right("yyyy'T'HH:mm:ss.SSS'Z'")
    )
    assertEquals(
      TimeFormat.fromIsoValue("1970T00:00:00.000").map(_.toString),
      Right("yyyy'T'HH:mm:ss.SSS")
    )
    assertEquals(
      TimeFormat.fromIsoValue("1970T00:00:00").map(_.toString),
      Right("yyyy'T'HH:mm:ss")
    )
    assertEquals(
      TimeFormat.fromIsoValue("1970T000000.000").map(_.toString),
      Right("yyyy'T'HHmmss.SSS")
    )
    assertEquals(
      TimeFormat.fromIsoValue("1970T000000").map(_.toString),
      Right("yyyy'T'HHmmss")
    )
    assertEquals(
      TimeFormat.fromIsoValue("1970T00:00").map(_.toString),
      Right("yyyy'T'HH:mm")
    )
    assertEquals(
      TimeFormat.fromIsoValue("1970T0000").map(_.toString),
      Right("yyyy'T'HHmm")
    )
    assertEquals(
      TimeFormat.fromIsoValue("1970T00").map(_.toString),
      Right("yyyy'T'HH")
    )
  }

  test("parsing various resolutions") {
    assertEquals(
      TimeFormat
        .fromExpression("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        .flatMap(_.parse("1970-01-01T00:00:00.000Z")),
      Right(0L)
    )
    assertEquals(
      TimeFormat
        .fromExpression("yyyy-MM-dd'T'HH:mm:ss.SSSS")
        .flatMap(_.parse("1970-01-01T00:00:00.0000")),
      Right(0L)
    )
    assertEquals(
      TimeFormat
        .fromExpression("yyyy-MM-dd'T'HH:mm:ss")
        .flatMap(_.parse("1970-01-01T00:00:00")),
      Right(0L)
    )
    assertEquals(
      TimeFormat
        .fromExpression("yyyy-MM-dd'T'HH:mm")
        .flatMap(_.parse("1970-01-01T00:00")),
      Right(0L)
    )
    assertEquals(
      TimeFormat
        .fromExpression("yyyy-MM-dd'T'HH")
        .flatMap(_.parse("1970-01-01T00")),
      Right(0L)
    )
    assertEquals(
      TimeFormat
        .fromExpression("yyyy-MM-dd")
        .flatMap(_.parse("1970-01-01")),
      Right(0L)
    )
    assertEquals(
      TimeFormat
        .fromExpression("yyyy-MM")
        .flatMap(_.parse("1970-01")),
      Right(0L)
    )
    assertEquals(
      TimeFormat.fromExpression("yyyy").flatMap(_.parse("1970")), Right(0L))
  }

  test("parse with no padding") {
    assertEquals(
      TimeFormat.fromExpression("y M d").flatMap(_.parse("1970 1 1")),
      Right(0L)
    )
  }

  test("parse with padding") {
    assertEquals(
      TimeFormat.fromExpression("y [ ]M d").flatMap(_.parse("1970  1 01")),
      Right(0L)
    )
  }

  test("fail to parse leap second") {
    // DateTimeFormatter fails to parse a leap second if not using LENIENT resolver.
    assert(TimeFormat.parseIso("2016-12-31T23:59:60").isLeft)
  }

  test("leniently parse leap second") {
    // With LENIENT resolution, DateTimeFormatter will simply roll a leap
    //   second over to the next day.
    // Note that the DateTimeFormatter.parsedLeapSecond TemporalQuery
    //   says that it will replace the "60" with "59", counter to this
    //   lenient resolution.
    val formatter = DateTimeFormatter.ISO_DATE_TIME.withResolverStyle(ResolverStyle.LENIENT)
    val t1 = LocalDateTime.parse("2016-12-31T23:59:60.123", formatter)
    val t2 = LocalDateTime.parse("2017-01-01T00:00:00.123", formatter)
    assertEquals(t1, t2)
  }
}
