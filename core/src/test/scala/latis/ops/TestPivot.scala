package latis.ops

import org.junit._
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite
import latis.data._

class TestPivot extends JUnitSuite {

  @Test
  def rgb = {
    val spectrum = SampledFunction.fromSeq(Seq(
      Sample(DomainData(1), RangeData(11)),
      Sample(DomainData(2), RangeData(22)),
      Sample(DomainData(3), RangeData(33))
    ))
    val f = SampledFunction.fromSeq(Seq(
      Sample(DomainData(0), RangeData(spectrum))
    ))
    
    val pivot = Pivot(Seq(1,2,3), null)
    val f2 = pivot.applyToData(f, null)
    f2.unsafeForce.samples.head match {
      case Sample(DomainData(Number(d)), RangeData(Number(r), Number(g), Number(b))) =>
        assertEquals(11, r, 0)
        assertEquals(22, g, 0)
        assertEquals(33, b, 0)
    }
  }
}
