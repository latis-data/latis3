package latis.ops

import cats.effect.unsafe.implicits.global
import org.scalatest.EitherValues._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Inside._

import latis.data._
import latis.dsl._
import latis.model._
import latis.util.Identifier.IdentifierStringContext

class ProjectionSuite extends AnyFunSuite {

  test("Project two from Tuple") {
    ModelParser.parse("(a, b, c)").foreach { model =>
      inside (Projection.fromExpression("a,b").value.applyToModel(model)) {
        case Right(Tuple(a: Scalar, b: Scalar)) =>
          assert(a.id == id"a")
          assert(b.id == id"b")
      }
    }
  }

  test("Project one from Tuple") {
    //Note: Tuple reduced to Scalar
    ModelParser.parse("(a, b, c)").foreach { model =>
      inside (Projection.fromExpression("b").value.applyToModel(model)) {
        case Right(b: Scalar) =>
          assert(b.id == id"b")
      }
    }
  }

  test("Reduce nested tuple") {
    ModelParser.parse("(a, (b, c))").foreach { model =>
      inside (Projection.fromExpression("a,b").value.applyToModel(model)) {
        case Right(Tuple(a: Scalar, b: Scalar)) =>
          assert(a.id == id"a")
          assert(b.id == id"b")
      }
    }
  }

  test("Project named nested tuple") {
    ModelParser.parse("(a, t:(b, c))").foreach { model =>
      inside (Projection.fromExpression("t").value.applyToModel(model)) {
        case Right(t: Tuple) =>
          assert(t.id == id"t")
      }
    }
  }

  test("Project one range variable") {
    val ds = DatasetGenerator("x -> (a, b: string)")
      .project("x,b")
    inside (ds.model) {
      case Function(d: Scalar, r: Scalar) =>
        assert(d.id == id"x")
        assert(r.id == id"b") //Note: tuple reduced to scalar
    }
    inside (ds.samples.compile.toList.unsafeRunSync().head) {
      case Sample(DomainData(Integer(x)), RangeData(Text(b))) =>
        assert(x == 0)
        assert(b == "a")
    }
  }

  test("Replace unprojected domain variable with Index") {
    val ds = DatasetGenerator("x: double -> a")
      .project("a")
    inside (ds.model) {
      case Function(_: Index, a: Scalar) =>
        assert(a.id == id"a")
    }
    inside (ds.samples.compile.toList.unsafeRunSync().head) {
      case Sample(DomainData(), RangeData(Integer(a))) =>
        assert(a == 0L)
    }
  }

  test("Replace unprojected 2D domain variable with single Index") {
    val ds = DatasetGenerator("(x, y) -> a")
      .project("a")
      .drop(2)
    inside (ds.model) {
      case Function(i: Index, a: Scalar) =>
        assert(i.id == id"_ix_y")
        assert(a.id == id"a")
    }
    inside (ds.samples.compile.toList.unsafeRunSync().head) {
      case Sample(DomainData(), RangeData(Integer(a))) =>
        assert(a == 2L)
    }
  }

  test("Project domain only") {
    val ds = DatasetGenerator("x: double -> a")
      .project("x")
    inside (ds.model) {
      case Function(_: Index, x: Scalar) =>
        assert(x.id == id"x")
    }
    inside (ds.samples.compile.toList.unsafeRunSync().head) {
      case Sample(DomainData(), RangeData(Real(x))) =>
        assert(x == 0.0)
    }
  }
}
