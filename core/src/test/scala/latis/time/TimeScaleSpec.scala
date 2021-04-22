package latis.time

import java.util.Date

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.units.UnitConverter

class TimeScaleSpec extends AnyFlatSpec {
  
  val timeScaleFromFormat = TimeScale.fromExpression("yyyy-MM-dd").toTry.get
  
  "A time scale from a format" should "have the default epoch" in {
    timeScaleFromFormat.epoch should be (new Date(0l))
  }
  
  it should "have the default time unit" in {
    timeScaleFromFormat.timeUnit should be (TimeUnit(0.001))
  }
  
  it should "have the default zero" in {
    timeScaleFromFormat.zero should be (0.0)
  }
  
  
  val numericTimeScale = TimeScale.fromExpression("hours since 1970-002").toTry.get
  
  "A numeric time scale" should "have the correct units" in {
    numericTimeScale.timeUnit should be (TimeUnit(3600))
  }
  
  it should "have the correct zero" in {
    numericTimeScale.zero should be (-24)
  }
  
  
  val timeConverter = UnitConverter(
    TimeScale.fromExpression("seconds since 2000-01-01T00:00:01").toTry.get,
    TimeScale.fromExpression("milliseconds since 2000-01-01").toTry.get
  )
  
  "A time converter" should "convert between numeric time scales" in {
    val z = timeConverter.convert(1) 
    z should be (2000.0)
  }

  "A Julian TimeScale" should "support conversion to Java time" in {
    val tc = UnitConverter(TimeScale.Julian, TimeScale.Default)
    tc.convert(2440587.5) should be (0) // 1970-01-01
  }

  it should "support conversion from Java time" in {
    val tc = UnitConverter(TimeScale.Default, TimeScale.Julian)
    tc.convert(0) should be (2440587.5) // 1970-01-01
  }

  it should "be constructed from Julian units" in {
    TimeScale.fromExpression("julian").fold(fail(_), identity) should be (TimeScale.Julian)
    TimeScale.fromExpression("Julian Date").fold(fail(_), identity) should be (TimeScale.Julian)
  }

  it should "have the correct string representation" in {
    TimeScale.Julian.toString should be ("Julian Date")
  }
}
