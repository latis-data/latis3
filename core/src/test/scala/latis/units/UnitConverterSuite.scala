package latis.units

import munit.FunSuite

import latis.time.TimeScale

class UnitConverterSuite extends FunSuite {

  test("convert time units") {
    val ts1 = TimeScale.Default
    val ts2 = TimeScale
      .fromExpression("seconds since 1970")
      .getOrElse(fail("failed to create TimeScale"))

    UnitConverter.fromScales(ts1, ts2).fold(
      le => fail(le.message, le),
      c  => assertEquals(c.convert(1000), 1.0)
    )
  }
}
