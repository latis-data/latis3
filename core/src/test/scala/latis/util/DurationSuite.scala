package latis.util

import java.time.LocalDateTime

import cats.syntax.all._

class DurationSuite extends munit.FunSuite {

  test("add a year") {
    assertEquals(
      LocalDateTime.of(2022, 1, 1, 0, 0, 0).plus(Duration.of(years = 1)),
      LocalDateTime.of(2023, 1, 1, 0, 0, 0)
    )
  }

  test("add a month") {
    assertEquals(
      LocalDateTime.of(2022, 1, 1, 0, 0, 0).plus(Duration.of(months = 1)),
      LocalDateTime.of(2022, 2, 1, 0, 0, 0)
    )
  }

  test("add a day") {
    assertEquals(
      LocalDateTime.of(2022, 1, 1, 0, 0, 0).plus(Duration.of(days = 1)),
      LocalDateTime.of(2022, 1, 2, 0, 0, 0)
    )
  }

  test("add an hour") {
    assertEquals(
      LocalDateTime.of(2022, 1, 1, 0, 0, 0).plus(Duration.of(hours = 1)),
      LocalDateTime.of(2022, 1, 1, 1, 0, 0)
    )
  }

  test("add a minute") {
    assertEquals(
      LocalDateTime.of(2022, 1, 1, 0, 0, 0).plus(Duration.of(minutes = 1)),
      LocalDateTime.of(2022, 1, 1, 0, 1, 0)
    )
  }

  test("add a second") {
    assertEquals(
      LocalDateTime.of(2022, 1, 1, 0, 0, 0).plus(Duration.of(seconds = 1)),
      LocalDateTime.of(2022, 1, 1, 0, 0, 1)
    )
  }

  test("subtract a year") {
    assertEquals(
      LocalDateTime.of(2022, 1, 1, 0, 0, 0).minus(Duration.of(years = 1)),
      LocalDateTime.of(2021, 1, 1, 0, 0, 0)
    )
  }

  test("subtract a month") {
    assertEquals(
      LocalDateTime.of(2022, 1, 1, 0, 0, 0).minus(Duration.of(months = 1)),
      LocalDateTime.of(2021, 12, 1, 0, 0, 0)
    )
  }

  test("subtract a day") {
    assertEquals(
      LocalDateTime.of(2022, 1, 1, 0, 0, 0).minus(Duration.of(days = 1)),
      LocalDateTime.of(2021, 12, 31, 0, 0, 0)
    )
  }

  test("subtract an hour") {
    assertEquals(
      LocalDateTime.of(2022, 1, 1, 0, 0, 0).minus(Duration.of(hours = 1)),
      LocalDateTime.of(2021, 12, 31, 23, 0, 0)
    )
  }

  test("subtract a minute") {
    assertEquals(
      LocalDateTime.of(2022, 1, 1, 0, 0, 0).minus(Duration.of(minutes = 1)),
      LocalDateTime.of(2021, 12, 31, 23, 59, 0)
    )
  }

  test("subtract a second") {
    assertEquals(
      LocalDateTime.of(2022, 1, 1, 0, 0, 0).minus(Duration.of(seconds = 1)),
      LocalDateTime.of(2021, 12, 31, 23, 59, 59)
    )
  }

  test("construct from an ISO duration") {
    assertEquals(
      Duration.fromIsoString("P1Y2M3DT4H5M6S"),
      Duration.of(1, 2, 3, 4, 5, 6).asRight
    )
    assertEquals(
      Duration.fromIsoString("P1DT5S"),
      Duration.of(days = 1, seconds = 5).asRight
    )
  }

  test("construct from an ISO duration without T segment") {
    assertEquals(
      Duration.fromIsoString("P3Y2M1D"),
      Duration.of(3, 2, 1, 0, 0, 0).asRight
    )
    assertEquals(
      Duration.fromIsoString("P1D"),
      Duration.of(days = 1).asRight
    )
  }

  test("construct from an ISO duration with only T segment") {
    assertEquals(
      Duration.fromIsoString("PT1H2M3S"),
      Duration.of(0, 0, 0, 1, 2, 3).asRight
    )
    assertEquals(
      Duration.fromIsoString("PT30M"),
      Duration.of(minutes = 30).asRight
    )
  }

  test("reject invalid ISO durations") {
    // Some duration is required
    assert(Duration.fromIsoString("").isLeft)
    assert(Duration.fromIsoString("P").isLeft)
    assert(Duration.fromIsoString("PT").isLeft)

    // Must be in descending order
    assert(Duration.fromIsoString("P1D2M").isLeft)
    assert(Duration.fromIsoString("PT1S2M").isLeft)
  }
}

