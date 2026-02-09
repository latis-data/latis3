package latis.util

import munit.FunSuite

class BoundsSuite extends FunSuite {

  val bounds: Bounds[Double] = Bounds.of(-1.0, 2.0)
  val inclusive: Bounds[Double] = Bounds.inclusive(-1.0, 2.0)

  test("contains without edge cases") {
    assert(bounds.contains(0))      //yes
    assert(!bounds.contains(-2))    //too low
    assert(!bounds.contains(3))     //too high
    assert(inclusive.contains(0))   //yes
    assert(!inclusive.contains(-2)) //too low
    assert(!inclusive.contains(3))  //too high
  }

  test("does not contain NaN") {
    assert(!bounds.contains(Double.NaN))
  }

  test("contains lower bound") {
    assert(bounds.contains(-1))
  }

  test("default bounds does not contain upper bound") {
    assert(!bounds.contains(2))
  }

  test("contains upper bound if inclusive") {
    assert(inclusive.contains(2))
  }

  test("empty if upper less than lower") {
    assert(Bounds.of(1, 0).isEmpty)
  }

  test("point contains") {
    assert(PointBounds(0).contains(0))
  }

  test("point does not contain") {
    assert(! PointBounds(0).contains(1))
  }

  test("equal inclusive bounds contains") {
    assert(Bounds.of(0, 0, true).contains(0))
  }

  test("equal exclusive bounds does not contain") {
    assert(! Bounds.of(0, 0).contains(0))
  }

  test("empty if equal and not inclusive") {
    assert(Bounds.of(0, 0).isEmpty)
  }

  test("empty if NaN") {
    assert(Bounds.of(Double.NaN, 0d).isEmpty)
    assert(Bounds.of(0d, Double.NaN).isEmpty)
    assert(Bounds.of(Double.NaN, Double.NaN).isEmpty)
  }

  test("infinite bounds") {
    val infBounds = Bounds.unbounded[Double]
    assert(infBounds.contains(0.0))
    assert(infBounds.contains(Double.NegativeInfinity))
    assert(infBounds.contains(Double.PositiveInfinity))
    assert(!infBounds.contains(Double.NaN))
    assert(infBounds.constrainLower(1.0).contains(1.0)) //inclusive
    assert(! infBounds.constrainUpper(1.0).contains(1.0)) //exclusive
  }

  test("string bounds") {
    val sBounds = Bounds.of("b", "d")
    assert(sBounds.contains("c"))
    assert(!sBounds.contains("a"))
    assert(!sBounds.contains("e"))
    assert(sBounds.contains("b"))
    assert(!sBounds.contains("d"))
    assert(Bounds.inclusive("b", "d").contains("d"))
    assert(!Bounds.inclusive("b", "d").contains("dog"))
  }

  test("extract bounds") {
    bounds match {
      case NonEmptyBounds(ClosedBound(l), OpenBound(u)) =>
        assert(l == -1D)
        assert(u == 2D)
      case _ => fail("unexpected bounds")
    }
  }

  test("constrain lower bound") {
    bounds.constrainLower(0) match {
      case NonEmptyBounds(ClosedBound(v), _) => assert(v == 0D)
      case _ => fail("Expected closed lower bound")
    }
  }

  test("don't constrain lower bound") {
    bounds.constrainLower(-2) match {
      case NonEmptyBounds(ClosedBound(v), _) => assert(v == -1D)
      case _ => fail("Expected closed lower bound")
    }
  }

  test("constrain upper bound") {
    bounds.constrainUpper(1) match {
      case NonEmptyBounds(_, OpenBound(v)) => assert(v == 1D)
      case _ => fail("Expected open upper bound")
    }
  }

  test("don't constrain upper bound") {
    bounds.constrainUpper(3) match {
      case NonEmptyBounds(_, OpenBound(v)) => assert(v == 2D)
      case _ => fail("Expected open upper bound")
    }
  }

  test("lower constraint greater that upper bound") {
    assert(bounds.constrainLower(3).isEmpty)
  }

  test("upper constraint lower that lower bound") {
    assert(bounds.constrainUpper(-2).isEmpty)
  }

  test("Bound equality") {
    assertEquals(bounds, Bounds.of(-1.0, 2.0))
  }

  test("empty Bounds equality") {
    assertEquals(Bounds.empty[Int], Bounds.empty[Int])
  }

  test("unbounded Bounds equality") {
    assertEquals(Bounds.unbounded[Int], Bounds.unbounded[Int])
  }

  test("to string") {
    assert(Bounds.of('a', 'c').toString == "[a,c)")
  }
}
