package latis.util

import munit.FunSuite

class BoundsSuite extends FunSuite {

  val bounds: Bounds[Double] = Bounds.of(-1.0, 2.0).get

  test("contains without edge cases") {
    assert(bounds.contains(0))   //yes
    assert(!bounds.contains(-2)) //too low
    assert(!bounds.contains(3))  //too high
    //with inclusive = true
    assert(bounds.contains(0, true))   //yes
    assert(!bounds.contains(-2, true)) //too low
    assert(!bounds.contains(3, true))  //too high
  }

  test("does not contain NaN") {
    assert(!bounds.contains(Double.NaN))
  }

  test("contains lower bound") {
    assert(bounds.contains(-1))
  }

  test("does not contain upper bound") {
    assert(!bounds.contains(2))
  }

  test("contains upper bound if inclusive") {
    assert(bounds.contains(2, true))
  }

  test("invalid upper less than lower") {
    assert(Bounds.of(1, 0).isEmpty)
  }

  test("invalid if equal") {
    assert(Bounds.of(0, 0).isEmpty)
  }

  test("invalid if NaN") {
    assert(Bounds.of(Double.NaN, 0d).isEmpty)
    assert(Bounds.of(0d, Double.NaN).isEmpty)
    assert(Bounds.of(Double.NaN, Double.NaN).isEmpty)
  }

  test("infinite bounds") {
    val infBounds = Bounds.of(Double.NegativeInfinity, Double.PositiveInfinity).get
    assert(infBounds.contains(0))
    assert(infBounds.contains(Double.NegativeInfinity))
    assert(!infBounds.contains(Double.PositiveInfinity))
    assert(!infBounds.contains(Double.NaN))
  }

  test("string bounds") {
    val sBounds = Bounds.of("b", "d").get
    assert(sBounds.contains("c"))
    assert(!sBounds.contains("a"))
    assert(!sBounds.contains("e"))
    assert(sBounds.contains("b"))
    assert(!sBounds.contains("d"))
    assert(sBounds.contains("d", true))
    assert(!sBounds.contains("dog", true))
  }

  test("extract bounds") {
    bounds match {
      case Bounds(l, u) =>
        assert(l == -1D)
        assert(u == 2D)
    }
  }

  test("constrain lower bound") {
    bounds.constrainLower(0).get match {
      case Bounds(v, _) => assert(v == 0D)
    }
  }

  test("don't constrain lower bound") {
    bounds.constrainLower(-2).get match {
      case Bounds(v, _) => assert(v == -1D)
    }
  }

  test("constrain upper bound") {
    bounds.constrainUpper(1).get match {
      case Bounds(_, v) => assert(v == 1D)
    }
  }

  test("don't constrain upper bound") {
    bounds.constrainUpper(3).get match {
      case Bounds(_, v) => assert(v == 2D)
    }
  }

  test("lower constraint greater that upper bound") {
    assert(bounds.constrainLower(3).isEmpty)
  }

  test("upper constraint lower that lower bound") {
    assert(bounds.constrainUpper(-2).isEmpty)
  }

  test("Bound equality") {
    assert(bounds == Bounds.of(-1, 2).get)
  }

  test("to string") {
    assert(Bounds.of('a', 'c').get.toString == "Bounds(a,c)")
  }
}
