package latis.time

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import latis.units.UnitConverter

class TimeScaleSpec extends AnyFlatSpec {
  val timeScaleFromFormat = TimeScale("yyyy-MM-dd")

  "A time scale from a format" should "have the default epoch" in {
    timeScaleFromFormat.epoch should be("1970-01-01")
  }

  it should "have the default time unit" in {
    timeScaleFromFormat.timeUnit should be(TimeUnit(0.001))
  }

  it should "have the default zero" in {
    timeScaleFromFormat.zero should be(0.0)
  }

  val numericTimeScale = TimeScale("hours since 1970-002")

  "A numeric time scale" should "have the correct units" in {
    numericTimeScale.timeUnit should be(TimeUnit(3600))
  }

  it should "have the correct zero" in {
    numericTimeScale.zero should be(-24)
  }

  val timeConverter = UnitConverter(
    TimeScale("seconds since 2000-01-01T00:00:01"),
    TimeScale("milliseconds since 2000-01-01")
  )

  "A time converter" should "convert between numeric time scales" in {
    val z = timeConverter.convert(1)
    z should be(2000.0)
  }
}
