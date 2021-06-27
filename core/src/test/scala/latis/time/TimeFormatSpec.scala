package latis.time

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class TimeFormatSpec extends AnyFlatSpec {
  
  "TimeFormat" should "parse an ISO string" in {
    TimeFormat.parseIso("1970-001T00:00:01") should be (Right(1000))
  }
  
  it should "provide a default ISO string" in {
    val time = 1000L * (86400 + 3600 + 61)
    TimeFormat.formatIso(time) should be ("1970-01-02T01:01:01.000Z")
  }
  
  it should "parse a formatted time string" in {
    TimeFormat.fromExpression("yyyyDDD").flatMap( _.parse("1970002")) should be (Right(86400000))

  }
  
  it should "format a time value" in {
    TimeFormat.fromExpression("yyyy MMM dd HH:mm").map(_.format(3600000)) should be (Right("1970 Jan 01 01:00"))
  }
  
  it should "use a specified century for 2-digit years" in {
    val time: Long =  TimeFormat.fromExpression("yyMMdd")
      .flatMap(_.setCenturyStart("3000").parse("330501")).getOrElse(0L)
    TimeFormat.fromExpression("yyyy-MM-dd").map(_.format(time)) should be (Right("3033-05-01"))
  }

  it should "have a string representation" in {
    TimeFormat.Iso.toString should be ("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  }
}
