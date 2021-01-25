package latis.data

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class LinearSetSpec extends AnyFlatSpec {

  val set1D = LinearSet1D(0.0, 1.0, 10)

  "A 1D linear set" should "have a min" in {
    set1D.min match {
      case DomainData(Number(d)) =>
        d should be(0.0)
    }
  }

  it should "have a max" in {
    set1D.max match {
      case DomainData(Number(d)) =>
        d should be(9.0)
    }
  }

  it should "have a length" in {
    set1D.length should be(10)
  }

  it should "provide an index of a set element" in {
    set1D.indexOf(DomainData(3.0)) should be(3)
  }

  it should "provide an index of -1 for values not equal to a set element" in {
    set1D.indexOf(DomainData(3.3)) should be(-1)
  }


  val set2D = LinearSet2D(set1D, set1D)

  "A 2D linear set" should "have a min" in {
    set2D.min match {
      case DomainData(Number(d1), Number(d2)) =>
        d1 should be (0.0)
        d2 should be (0.0)
    }
  }

  "A 2D linear set" should "have a max" in {
    set2D.max match {
      case DomainData(Number(d1), Number(d2)) =>
        d1 should be (9.0)
        d2 should be (9.0)
    }
  }
}
