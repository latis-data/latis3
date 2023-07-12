package latis.ops

import munit.CatsEffectSuite

import latis.data._
import latis.dsl._
import latis.model._
import latis.util.Identifier._

class UncurrySuite extends CatsEffectSuite {

  // x -> y -> a
  val dataset2D = DatasetGenerator("(x, y) -> a").curry(1)

  test("uncurry nested dataset") {
    val ds = dataset2D.uncurry()
    ds.model match {
      case Function(Tuple(x: Scalar, y: Scalar), a: Scalar) =>
        assertEquals(x.id, id"x")
        assertEquals(y.id, id"y")
        assertEquals(a.id, id"a")
      case _ => fail("unexpected model")
    }
    //Test all "a" to make sure order is preserved
    ds.samples.map {
      case Sample(DomainData(_, _), RangeData(Integer(a))) => a
      case _ => fail("")
    }.compile.toList.assertEquals(List(0L, 1L, 2L, 3L, 4L, 5L))
  }

  test("uncurry multiple nested functions".ignore) {}

  //TODO: prevent 2D domain with one Index until we support Cartesian
  test("uncurry with index".ignore) {
    // _i -> y -> a
    val model = Function.from(
      Index(id"_i"),
      ModelParser.unsafeParse("y -> a")
    ).fold(fail("failed to construct function", _), identity)
    Uncurry().applyToModel(model).map {
      println(_)
    }
  }
}
