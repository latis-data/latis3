package latis.data

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.Inside.inside

class BinSetSpec extends AnyFlatSpec {

  //Note, bin center semantics
  val set1D = BinSet1D(0.0, 1.0, 10)

  "A 1D bin set" should "have a min" in {
    inside(set1D.min) {
      case DomainData(Number(d)) =>
        d should be(0.0)
    }
  }

  it should "have a max" in {
    inside(set1D.max) {
      case DomainData(Number(d)) =>
        d should be(9.0)
    }
  }

  it should "have a length" in {
    set1D.length should be(10)
  }

  it should "provide an index of a bin for an edge value" in {
    set1D.indexOf(DomainData(3.0)) should be(3)
  }

  it should "provide an index of a bin" in {
    set1D.indexOf(DomainData(3.3)) should be(3)
  }


  val set2D = BinSet2D(set1D, set1D)

  "A 2D linear set" should "have a min" in {
    inside(set2D.min) {
      case DomainData(Number(d1), Number(d2)) =>
        d1 should be(0.0)
        d2 should be(0.0)
    }
  }

  "A 2D linear set" should "have a max" in {
    inside(set2D.max) {
      case DomainData(Number(d1), Number(d2)) =>
        d1 should be(9.0)
        d2 should be(9.0)
    }
  }

  it should "provide an index of a bin" in {
    set2D.indexOf(DomainData(1.3, 2.0)) should be(12)
  }
}
