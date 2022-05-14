package latis.time

import munit.FunSuite

class TimeFormatSuite extends FunSuite {

  test("format to default ISO string") {
    val time = 1000L * (86400 + 3600 + 61)
    assertEquals(TimeFormat.formatIso(time), "1970-01-02T01:01:01.000Z")
  }

  test("parse a formatted time string") {
    assertEquals(
      TimeFormat.fromExpression("yyyyDDD").flatMap( _.parse("1970002")).getOrElse(fail("time not generated")),
      86400000L
    )
  }

  test("parse a formatted time string with literals") {
    assertEquals(
      TimeFormat.fromExpression("yyyy'T'HH'Z'").flatMap( _.parse("1970T00Z")).getOrElse(fail("time not generated")),
      0L
    )
  }
  
  test("format a time value") {
    assertEquals(
      TimeFormat.fromExpression("yyyy MMM dd HH:mm").map(_.format(3600000)).getOrElse(fail("time not generated")),
      "1970 Jan 01 01:00"
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
    assertEquals(TimeFormat.fromIsoValue("1970-01-01").getOrElse(fail("time not generated")).toString, "yyyy-MM-dd")
    assertEquals(TimeFormat.fromIsoValue("19700101").getOrElse(fail("time not generated")).toString  , "yyyyMMdd")
    assertEquals(TimeFormat.fromIsoValue("1970001").getOrElse(fail("time not generated")).toString   , "yyyyDDD")
    assertEquals(TimeFormat.fromIsoValue("1970-001").getOrElse(fail("time not generated")).toString  , "yyyy-DDD")
    assertEquals(TimeFormat.fromIsoValue("1970-01").getOrElse(fail("time not generated")).toString   , "yyyy-MM")
    assertEquals(TimeFormat.fromIsoValue("1970").getOrElse(fail("time not generated")).toString      , "yyyy")
  }

  test("make TimeFormat from ISO time") {
    assertEquals(TimeFormat.fromIsoValue("1970T00:00:00.000Z").getOrElse(fail("time not generated")).toString, "yyyy'T'HH:mm:ss.SSS'Z'")
    assertEquals(TimeFormat.fromIsoValue("1970T00:00:00.000").getOrElse(fail("time not generated")).toString , "yyyy'T'HH:mm:ss.SSS")
    assertEquals(TimeFormat.fromIsoValue("1970T00:00:00").getOrElse(fail("time not generated")).toString     , "yyyy'T'HH:mm:ss")
    assertEquals(TimeFormat.fromIsoValue("1970T000000.000").getOrElse(fail("time not generated")).toString   , "yyyy'T'HHmmss.SSS")
    assertEquals(TimeFormat.fromIsoValue("1970T000000").getOrElse(fail("time not generated")).toString       , "yyyy'T'HHmmss")
    assertEquals(TimeFormat.fromIsoValue("1970T00:00").getOrElse(fail("time not generated")).toString        , "yyyy'T'HH:mm")
    assertEquals(TimeFormat.fromIsoValue("1970T0000").getOrElse(fail("time not generated")).toString         , "yyyy'T'HHmm")
    assertEquals(TimeFormat.fromIsoValue("1970T00").getOrElse(fail("time not generated")).toString           , "yyyy'T'HH")
  }

  test("parsing various resolutions") {
    assertEquals(TimeFormat.fromExpression("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").getOrElse(fail("time not generated"))
      .parse("1970-01-01T00:00:00.000Z").getOrElse(fail("time not generated")), 0L)
    assertEquals(TimeFormat.fromExpression("yyyy-MM-dd'T'HH:mm:ss.SSS").getOrElse(fail("time not generated"))
      .parse("1970-01-01T00:00:00.000").getOrElse(fail("time not generated")), 0L)
    assertEquals(TimeFormat.fromExpression("yyyy-MM-dd'T'HH:mm:ss").getOrElse(fail("time not generated"))
      .parse("1970-01-01T00:00:00").getOrElse(fail("time not generated")), 0L)
    assertEquals(TimeFormat.fromExpression("yyyy-MM-dd'T'HH:mm").getOrElse(fail("time not generated"))
      .parse("1970-01-01T00:00").getOrElse(fail("time not generated")), 0L)
    assertEquals(TimeFormat.fromExpression("yyyy-MM-dd'T'HH").getOrElse(fail("time not generated"))
      .parse("1970-01-01T00").getOrElse(fail("time not generated")), 0L)
    assertEquals(TimeFormat.fromExpression("yyyy-MM-dd").getOrElse(fail("time not generated"))
      .parse("1970-01-01").getOrElse(fail("time not generated")), 0L)
    assertEquals(TimeFormat.fromExpression("yyyy-MM").getOrElse(fail("time not generated"))
      .parse("1970-01").getOrElse(fail("time not generated")), 0L)
    assertEquals(TimeFormat.fromExpression("yyyy").getOrElse(fail("time not generated"))
      .parse("1970").getOrElse(fail("time not generated")), 0L)
  }

}
