package latis.data

import munit.FunSuite

import Data._

class CartesianFunctionSuite extends FunSuite {

  test("CartesianFunction should evaluate") {
    val xs: IndexedSeq[Datum] = Vector(1.1, 2.2, 3.3).map(DoubleValue)
    val x: Datum = DoubleValue(2.2)

    CartesianFunction1D.fromData(xs, xs).fold(
      fail("failed to make function", _),
      f => assertEquals(f.eval(DomainData(x)), Right(RangeData(2.2)))
    )
  }
}
