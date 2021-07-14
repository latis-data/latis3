package latis.ops

import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Inside.inside

import latis.data._
import latis.dsl._
import latis.model._

class TransposeSuite extends AnyFunSuite {

  val ds = DatasetGenerator("(x, y) -> a")
  // 2 x 3 x-y grid:
  //   2    5
  //   1    4
  //   0    3

  test("model domain transposed") {
    inside(ds.transpose().model) {
      case Function(Tuple(y: Scalar, x: Scalar), a: Scalar) =>
        assert(y.id.asString == "y")
        assert(x.id.asString == "x")
        assert(a.id.asString == "a")
    }
  }

  test("samples are reordered") {
    val as = ds.transpose().samples.map {
      case Sample(_, RangeData(Integer(a))) => a
      case _ => 0
    }.compile.toList.unsafeRunSync()
    assert(as == List(0,3,1,4,2,5))
  }

}
