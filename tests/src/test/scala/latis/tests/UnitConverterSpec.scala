package latis.tests

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.time.TimeScale
import latis.units.UnitConverter

class UnitConverterSpec extends FlatSpec {

  "A UnitConverter" should "convert time units" in {
    val ts1 = TimeScale.Default
    val ts2 = TimeScale("seconds since 1970")
    UnitConverter(ts1, ts2).convert(1000) should be (1.0)
  }
}
