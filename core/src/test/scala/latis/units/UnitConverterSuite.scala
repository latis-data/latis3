package latis.units

import munit.FunSuite

import latis.time.TimeScale

class UnitConverterSuite extends FunSuite {

  test("convert time units") {
    val ts1 = TimeScale.Default
    val ts2 = TimeScale.fromExpression("seconds since 1970").getOrElse(fail("Time Scale not generated"))
    assertEquals(UnitConverter(ts1, ts2).convert(1000), 1.0)
  }
}
