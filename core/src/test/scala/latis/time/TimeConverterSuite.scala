package latis.time

import java.util.Date

import munit.FunSuite

class TimeConverterSuite extends FunSuite {

  private def makeDate(iso: String): Date = {
    TimeFormat.parseIso(iso)
      .map(new Date(_))
      .fold(fail(s"Invalid ISO 8601 time: $iso", _), identity)
  }

  private val gpsEpoch  = makeDate("1980-01-06")
  private val epoch1981 = makeDate("1981-06-30") // day before leap second
  private val epoch1982 = makeDate("1982-06-30") // day before leap second

  private val gpsMicros = TimeScale(TimeUnit.Microseconds, gpsEpoch, TimeScaleType.Atomic)
  private val civil1981 = TimeScale(TimeUnit.Seconds, epoch1981, TimeScaleType.Civil)
  private val civil1982 = TimeScale(TimeUnit.Seconds, epoch1982, TimeScaleType.Civil)
  private val atomic1981 = TimeScale(TimeUnit.Seconds, epoch1981, TimeScaleType.Atomic)
  private val atomic1982 = TimeScale(TimeUnit.Seconds, epoch1982, TimeScaleType.Atomic)

  /*
    These tests emphasize the handling of leap seconds between epochs and
    before the target time. Note that the epochs used for most of these tests
    are one year apart and one day prior to a leap second being introduced.
    Note also that epochs are UTC instants on the civil time scale.

    The diagrams for each test show time lines (not to scale) extending to the
    right for the two time scales used in the conversion.
      "|" indicates the epoch (zero) of a time scale.
      "x" indicates the time being converted (necessarily aligned between the scales).
      "^" indicates the location of leap seconds.
      "+" indicates where a leap second is counted in a atomic time scale.
   */

  /*
    |--------x-->
         |---x-->
      ^    ^
   */
  test("No leap seconds for civil to civil conversions") {
    val converter = TimeConverter(civil1981, civil1982)
    val t = converter.convert(367d * 86400) // year + 2 days
    assertEquals(t, 2d * 86400) // 2 days, no leap seconds
  }

  /*
  |------------x-->
          |-+--x-->
    ^       ^
  */
  test("civil to atomic with time after later epoch") {
    val converter = TimeConverter(civil1981, atomic1982)
    val t = converter.convert(367d * 86400) // year + 2 days
    assertEquals(t, 2d * 86400 + 1) // 2 days + leap second
  }

  /*
  |---x----------->
      x   |-+----->
    ^       ^
  */
  test("civil to atomic with time before later epoch") {
    val converter = TimeConverter(civil1981, atomic1982)
    val t = converter.convert(2d * 86400) // 2 days
    assertEqualsDouble(t, - 363d * 86400 + 1, 1) // -year + 2 days + leap second between epochs
  }

  /*
          |----x-->
  |-+-------+--x-->
    ^       ^
  */
  test("civil to atomic with earlier epoch") {
    val converter = TimeConverter(civil1982, atomic1981)
    val t = converter.convert(2d * 86400) // 2 days
    assertEquals(t, 367d * 86400 + 2) // year + 2 days + 2 leap seconds
  }

  /*
      x   |------->
  |-+-x-----+----->
    ^       ^
  */
  test("civil to atomic with earlier epoch, negative") {
    val converter = TimeConverter(civil1982, atomic1981)
    val t = converter.convert(- 363d * 86400) // 1 year ago + 2 days
    assertEquals(t, 2d * 86400 + 1) // 2 days + leap second
  }

  /*
  |-+-------+--x-->
          |----x-->
    ^       ^
  */
  test("atomic to civil with time after later epoch") {
    val converter = TimeConverter(atomic1981, civil1982)
    val t = converter.convert(367d * 86400 + 2) // year + 2 days + 2 ls
    assertEquals(t, 2d * 86400) // 2 days
  }

  /*
  |-+-x-----+----->
      x   |------->
    ^       ^
  */
  test("atomic to civil with time before later epoch") {
    val converter = TimeConverter(atomic1981, civil1982)
    val t = converter.convert(2d * 86400 + 1) // 2 days + 1 ls
    assertEquals(t, - 363d * 86400) // -year + 2 days
  }

  /*
          |-+-x--->
  |-----------x--->
    ^       ^
  */
  test("atomic to civil with earlier epoch") {
    val converter = TimeConverter(atomic1982, civil1981)
    val t = converter.convert(2d * 86400 + 1) // 2 days + 1 ls
    assertEquals(t, 367d * 86400) // year + 2 days
  }

  /*
      x   |-+----->
  |---x------------>
    ^       ^
  */
  test("atomic to civil with earlier epoch, negative") {
    val converter = TimeConverter(atomic1982, civil1981)
    val t = converter.convert(-363d * 86400) // -year + 2 days
    assertEquals(t, 2d * 86400) // 2 days
  }

  /*
  |-+-------+--x--->
          |-+--x-->
    ^       ^
  */
  test("atomic to atomic with time after later epoch") {
    val converter = TimeConverter(atomic1981, atomic1982)
    val t = converter.convert(367d * 86400 + 2) // year + 2 days + 2 ls
    assertEquals(t, 2d * 86400 + 1) // 2 days + 1 ls
  }

  /*
  |-+--x----+------>
       x  |-+----->
    ^       ^
  */
  test("atomic to atomic with time before later epoch") {
    val converter = TimeConverter(atomic1981, atomic1982)
    val t = converter.convert(2d * 86400 + 1) // 2 days + 1 ls
    assertEquals(t, -363d * 86400) // -year + 2 days
  }

  /*
          |-+--x-->
  |-+-------+--x--->
    ^       ^
  */
  test("atomic to atomic with earlier epoch") {
    val converter = TimeConverter(atomic1982, atomic1981)
    val t = converter.convert(2d * 86400 + 1) // 2 days + 1 ls
    assertEquals(t, 367d * 86400 + 2) // year + 2 days + 2 ls
  }

  /*
       x  |-+----->
  |-+--x----+------>
    ^       ^
  */
  test("atomic to atomic with earlier epoch, negative") {
    val converter = TimeConverter(atomic1982, atomic1981)
    val t = converter.convert(-363d * 86400) // -year + 2 days
    assertEquals(t, 2d * 86400 + 1) // 2 days + ls
  }

  /*
  |x+-------+----->
   x      |-+----->
    ^       ^
  */
  test("time between epochs before leap second") {
    val converter = TimeConverter(atomic1981, atomic1982)
    val t = converter.convert(0.5 * 86400) // 1/2 day, before leap second
    assertEqualsDouble(t, -364.5 * 86400 - 1, 0.1) // -year + 1/2 day - ls
  }

  /*
   x      |-+----->
  |x+-------+----->
    ^       ^
  */
  test("time between epochs before leap second, negative") {
    val converter = TimeConverter(atomic1982, atomic1981)
    val t = converter.convert(-364.5 * 86400 -1) // -year + 1/2 day - ls
    assertEqualsDouble(t, 0.5 * 86400, 0.1) // -year + 1/2 days
  }

  test("Unix epoch to GPS") {
    /*
      -(10 years, 5 days, 2 leap days), minus 19 leap seconds
      (-(10 * 365 + 5 + 2) * 86400 - 19) * 1000000 = -3.15964819e14
     */
    val converter = TimeConverter(TimeScale.Default, gpsMicros)
    val t = converter.convert(0d)
    assertEquals(t, -3.15964819e14)
  }

  test("GPS to Unix epoch") {
    val converter = TimeConverter(gpsMicros, TimeScale.Default)
    val t = converter.convert(-3.15964819e14)
    assertEquals(t, 0d)
  }
}
