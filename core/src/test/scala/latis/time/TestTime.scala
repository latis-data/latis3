package latis.time

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField, TemporalAccessor}

import latis.units.UnitConverter

object TestTime extends App {

  //println(TimeFormat.parseIso("1970-01-02") - TimeFormat.parseIso("1970-01-01"))

  val ts1 = TimeScale(TimeUnit.fromName("milliseconds"), "1970-01-01")
  val ts2 = TimeScale(TimeUnit.fromName("milliseconds"), "1970-01-02")
  val tc = new UnitConverter(ts1, ts2)
  //println(tc.convert(86400000))
  
  

}
