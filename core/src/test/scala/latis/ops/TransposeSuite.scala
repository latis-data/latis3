package latis.ops

import munit.CatsEffectSuite

import latis.data.*
import latis.dsl.*
import latis.model.*

class TransposeSuite extends CatsEffectSuite {

  val ds = DatasetGenerator("(x, y) -> a")
  // 2 x 3 x-y grid:
  //   2    5
  //   1    4
  //   0    3

  test("model domain transposed") {
    ds.transpose().model match {
      case Function(Tuple(y: Scalar, x: Scalar), a: Scalar) =>
        assertEquals(y.id.asString, "y")
        assertEquals(x.id.asString, "x")
        assertEquals(a.id.asString, "a")
      case _ => fail("unexpected model")
    }
  }

  test("samples are reordered") {
    ds.transpose().samples.map {
      case Sample(_, RangeData(Integer(a))) => a
      case _ => 0
    }.compile.toList.assertEquals(List(0L, 3L, 1L, 4L, 2L, 5L))
  }

}
