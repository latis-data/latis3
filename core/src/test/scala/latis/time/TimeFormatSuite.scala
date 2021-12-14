package latis.time

import org.scalatest.EitherValues._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class TimeFormatSuite extends AnyFunSuite {

  test("format to default ISO string") {
    val time = 1000L * (86400 + 3600 + 61)
    TimeFormat.formatIso(time) should be ("1970-01-02T01:01:01.000Z")
  }

  test("parse a formatted time string") {
    TimeFormat.fromExpression("yyyyDDD").flatMap( _.parse("1970002")).value should be (86400000)
  }

  test("parse a formatted time string with literals") {
    TimeFormat.fromExpression("yyyy'T'HH'Z'").flatMap( _.parse("1970T00Z")).value should be (0)
  }
  
  test("format a time value") {
    TimeFormat.fromExpression("yyyy MMM dd HH:mm").map(_.format(3600000)).value should be ("1970 Jan 01 01:00")
  }
  
//  test("use a specified century for 2-digit years") {
//    val time: Long =  TimeFormat.fromExpression("yyMMdd")
//      .flatMap(_.setCenturyStart("3000").parse("330501")).getOrElse(0L)
//    TimeFormat.fromExpression("yyyy-MM-dd").map(_.format(time)) should be (Right("3033-05-01"))
//  }

  test("string representation") {
    TimeFormat.Iso.toString should be ("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  }

  test("make TimeFormat from ISO date") {
    TimeFormat.fromIsoValue("1970-01-01").value.toString should be ("yyyy-MM-dd")
    TimeFormat.fromIsoValue("19700101").value.toString   should be ("yyyyMMdd")
    TimeFormat.fromIsoValue("1970001").value.toString    should be ("yyyyDDD")
    TimeFormat.fromIsoValue("1970-001").value.toString   should be ("yyyy-DDD")
    TimeFormat.fromIsoValue("1970-01").value.toString    should be ("yyyy-MM")
    TimeFormat.fromIsoValue("1970").value.toString       should be ("yyyy")
  }

  test("make TimeFormat from ISO time") {
    TimeFormat.fromIsoValue("1970T00:00:00.000Z").value.toString should be ("yyyy'T'HH:mm:ss.SSS'Z'")
    TimeFormat.fromIsoValue("1970T00:00:00.000").value.toString  should be ("yyyy'T'HH:mm:ss.SSS")
    TimeFormat.fromIsoValue("1970T00:00:00").value.toString      should be ("yyyy'T'HH:mm:ss")
    TimeFormat.fromIsoValue("1970T000000.000").value.toString    should be ("yyyy'T'HHmmss.SSS")
    TimeFormat.fromIsoValue("1970T000000").value.toString        should be ("yyyy'T'HHmmss")
    TimeFormat.fromIsoValue("1970T00:00").value.toString         should be ("yyyy'T'HH:mm")
    TimeFormat.fromIsoValue("1970T0000").value.toString          should be ("yyyy'T'HHmm")
    TimeFormat.fromIsoValue("1970T00").value.toString            should be ("yyyy'T'HH")
  }

  test("parsing various resolutions") {
    TimeFormat.fromExpression("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").value.parse("1970-01-01T00:00:00.000Z").value should be (0L)
    TimeFormat.fromExpression("yyyy-MM-dd'T'HH:mm:ss.SSS").value.parse("1970-01-01T00:00:00.000").value should be (0L)
    TimeFormat.fromExpression("yyyy-MM-dd'T'HH:mm:ss").value.parse("1970-01-01T00:00:00").value should be (0L)
    TimeFormat.fromExpression("yyyy-MM-dd'T'HH:mm").value.parse("1970-01-01T00:00").value should be (0L)
    TimeFormat.fromExpression("yyyy-MM-dd'T'HH").value.parse("1970-01-01T00").value should be (0L)
    TimeFormat.fromExpression("yyyy-MM-dd").value.parse("1970-01-01").value should be (0L)
    TimeFormat.fromExpression("yyyy-MM").value.parse("1970-01").value should be (0L)
    TimeFormat.fromExpression("yyyy").value.parse("1970").value should be (0L)
  }

}
