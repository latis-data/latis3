package latis.time

import java.util.Date

import scala.collection.SortedSet

import munit.FunSuite

class LeapSecondSuite extends FunSuite {

  private def makeDate(iso: String): Date = {
    TimeFormat.parseIso(iso)
      .map(new Date(_))
      .fold(fail(s"Invalid ISO 8601 time: $iso", _), identity)
  }

  // Confirm that we can use Date as a key in a SortedMap
  test("Date ordering") {
    val set = SortedSet(new Date(2), new Date(3), new Date(4), new Date(1))
    assertEquals(set.head, new Date(1))
    assertEquals(set.last, new Date(4))
  }

  test("at leap second") {
    val ls = TimeConverter.getLeapSeconds(makeDate("2009-01-01"))
    assertEquals(ls, 34)
  }

  test("before leap second") {
    val ls = TimeConverter.getLeapSeconds(makeDate("2008-12-31T23:59:59"))
    assertEquals(ls, 33)
  }

  test("before UTC") {
    val ls = TimeConverter.getLeapSeconds(makeDate("1970-01-01"))
    assertEquals(ls, 0)
  }

  test("future") {
    val nextYear = new Date(new Date().getTime() + 31536000000L)
    val ls = TimeConverter.getLeapSeconds(nextYear)
    assertEquals(ls, 37)
  }
}
