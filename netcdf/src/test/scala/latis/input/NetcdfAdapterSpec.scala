
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import ucar.ma2.Range
import ucar.ma2.Section

import latis.input.NetcdfAdapter

class NetcdfAdapterSpec extends FlatSpec {

  "A section" should "be strided" in {
    val section = new Section("0:9:3, 1:4:2")
    val stride: Array[Int] = Array(2,2)
    val s = NetcdfAdapter.applyStride(section: Section, stride: Array[Int])
    s.toString should be ("0:6:6,1:1:4")
  }

  "A range with a stride" should "make a consistent last value" in {
    val range = new Range(1, 4, 2) // 1:3:2
    range.last should be (3)
  }
}
