package latis.ops

import cats.effect.unsafe.implicits.global
import org.scalatest.EitherValues._
import org.scalatest.Inside._
import org.scalatest.funsuite.AnyFunSuite

import latis.data._
import latis.dsl._
import latis.model._
import latis.util.Identifier.IdentifierStringContext

class UncurrySuite extends AnyFunSuite {

  // x -> y -> a
  val dataset2D = DatasetGenerator("(x, y) -> a").curry(1)

  test("uncurry nested dataset") {
    val ds = dataset2D.uncurry()
    inside(ds.model) {
      case Function(Tuple(x: Scalar, y: Scalar), a: Scalar) =>
        assert(x.id == id"x")
        assert(y.id == id"y")
        assert(a.id == id"a")
    }
    //Test all "a" to make sure order is preserved
    val as = ds.samples.map {
      case Sample(DomainData(_, _), RangeData(Integer(a))) => a
      case _ => fail()
    }.compile.toList.unsafeRunSync()
    assert(as == List(0,1,2,3,4,5))
  }

  ignore("uncurry multiple nested functions") {}

  //TODO: prevent 2D domain with one Index until we support Cartesian
  ignore("uncurry with index") {
    // _i -> y -> a
    val model = Function.from(
      Index(id"_i"),
      ModelParser.unsafeParse("y -> a")
    ).value
    Uncurry().applyToModel(model).map {
      println(_)
    }
  }
}
