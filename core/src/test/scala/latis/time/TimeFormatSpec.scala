package latis.time

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class TimeFormatSpec extends AnyFlatSpec {
  "TimeFormat" should "parse an ISO string" in {
    TimeFormat.parseIso("1970-001T00:00:01") should be(Right(1000))
  }

  it should "provide a default ISO string" in {
    val time = 1000L * (86400 + 3600 + 61)
    TimeFormat.formatIso(time) should be("1970-01-02T01:01:01.000Z")
  }

  it should "parse a formatted time string" in {
    TimeFormat("yyyyDDD").parse("1970002") should be(Right(86400000))
  }

  it should "format a time value" in {
    TimeFormat("yyyy MMM dd HH:mm").format(3600000) should be("1970 Jan 01 01:00")
  }

  it should "use a specified century for 2-digit years" in {
    val time = TimeFormat("yyMMdd").setCenturyStart("2100").parse("330501").getOrElse(0L)
    TimeFormat("yyyy-MM-dd").format(time) should be("2133-05-01")
  }
}
