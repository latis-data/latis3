package latis.time

import java.util.Date

import munit.FunSuite

import latis.units.UnitConverter

class TimeScaleSuite extends FunSuite {
  
  private lazy val timeScaleFromFormat = TimeScale.fromExpression("yyyy-MM-dd").getOrElse(fail("time not generated"))
  
  test("have the default epoch from a time scale from a format") {
    assertEquals(timeScaleFromFormat.epoch, new Date(0L))
  }
  
  test("have the default time unit from a time scale from a format") {
    assertEquals(timeScaleFromFormat.timeUnit, TimeUnit(0.001))
  }
  
  test("have the default zero from a time scale from a format") {
    assertEquals(timeScaleFromFormat.zero, 0.0)
  }


  private lazy val numericTimeScale = TimeScale.fromExpression("hours since 1970-002").getOrElse(fail("time not generated"))
  
  test("have the correct units from a numeric time scale") {
    assertEquals(numericTimeScale.timeUnit, TimeUnit(3600))
  }
  
  test("have the correct zero from a numeric time scale") {
    assertEquals(numericTimeScale.zero, -24.0)
  }


  private lazy val timeConverter = UnitConverter(
    TimeScale.fromExpression("seconds since 2000-01-01T00:00:01").getOrElse(fail("time not generated")),
    TimeScale.fromExpression("milliseconds since 2000-01-01").getOrElse(fail("time not generated"))
  )
  
  test("convert between numeric time scales from a time converter") {
    val z = timeConverter.convert(1)
    assertEquals(z, 2000.0)
  }

  test("support conversion to Java time from a Julian TimeScale") {
    val tc = UnitConverter(TimeScale.JulianDate, TimeScale.Default)
    assertEquals(tc.convert(2440587.5), 0.0) // 1970-01-01
  }

  test("support conversion from Java time from a Julian TimeScale") {
    val tc = UnitConverter(TimeScale.Default, TimeScale.JulianDate)
    assertEquals(tc.convert(0), 2440587.5) // 1970-01-01
  }

  test("be constructed from JD units from a Julian TimeScale") {
    assertEquals(TimeScale.fromExpression("JD").fold(e => fail(e.message), identity), TimeScale.JulianDate)
    assert(TimeScale.fromExpression("Julian Date").isLeft)
  }

  test("have the correct string representation from a Julian TimeScale") {
    assertEquals(TimeScale.JulianDate.toString(), "JD")
  }
}
