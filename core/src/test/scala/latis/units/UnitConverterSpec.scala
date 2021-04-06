package latis.units

import latis.time.TimeScale
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class UnitConverterSpec extends AnyFlatSpec {
  "A UnitConverter" should "convert time units" in {
    val ts1 = TimeScale.Default
    val ts2 = TimeScale("seconds since 1970")
    UnitConverter(ts1, ts2).convert(1000) should be(1.0)
  }
}
