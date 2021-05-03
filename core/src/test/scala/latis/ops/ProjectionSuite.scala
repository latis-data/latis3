package latis.ops

import org.scalatest.funsuite.AnyFunSuite

import latis.data._
import latis.model._
import latis.output.TextWriter
import latis.util.DatasetGenerator
import latis.util.Identifier.IdentifierStringContext

class ProjectionSuite extends AnyFunSuite {

  test("Project two from Tuple") {
    ModelParser.parse("(a, b, c)").foreach { model =>
      Projection.fromExpression("a,b").toTry.get.applyToModel(model) match {
        case Right(Tuple(a: Scalar, b: Scalar)) =>
          assert(a.id.get == id"a")
          assert(b.id.get == id"b")
      }
    }
  }

  test("Project one from Tuple") {
    //Note: Tuple reduced to Scalar
    ModelParser.parse("(a, b, c)").foreach { model =>
      Projection.fromExpression("b").toTry.get.applyToModel(model) match {
        case Right(b: Scalar) =>
          assert(b.id.get == id"b")
      }
    }
  }

  test("Reduce nested tuple") {
    ModelParser.parse("(a, (b, c))").foreach { model =>
      Projection.fromExpression("a,b").toTry.get.applyToModel(model) match {
        case Right(Tuple(a: Scalar, b: Scalar)) =>
          assert(a.id.get == id"a")
          assert(b.id.get == id"b")
      }
    }
  }

  test("Project named nested tuple") {
    ModelParser.parse("(a, t:(b, c))").foreach { model =>
      Projection.fromExpression("t").toTry.get.applyToModel(model) match {
        case Right(t: Tuple) =>
          assert(t.id.get == id"t")
      }
    }
  }

  test("Project one range variable") {
    val ds = DatasetGenerator("x -> (a, b: string)")
      .project("x,b")
    //TextWriter().write(ds)
    ds.model match {
      case Function(d, r) =>
        assert(d.id.get == id"x")
        assert(r.id.get == id"b") //Note: tuple reduced to scalar
    }
    ds.samples.compile.toList.unsafeRunSync().head match {
      case Sample(DomainData(Integer(x)), RangeData(Text(b))) =>
        assert(x == 0)
        assert(b == "a")
    }
  }

  test("Replace unprojected domain variable with Index") {
    val ds = DatasetGenerator("x: double -> a")
      .project("a")
    //TextWriter().write(ds)
    ds.model match {
      case Function(_: Index, a: Scalar) =>
        assert(a.id.get == id"a")
    }
    ds.samples.compile.toList.unsafeRunSync().head match {
      case Sample(DomainData(), RangeData(Integer(a))) =>
        assert(a == 0L)
    }
  }

  test("Replace unprojected 2D domain variable with single Index") {
    val ds = DatasetGenerator("(x, y) -> a")
      .project("a")
      .drop(2)
    //TextWriter().write(ds)
    ds.model match {
      case Function(_: Index, a: Scalar) =>
        assert(a.id.get == id"a")
    }
    ds.samples.compile.toList.unsafeRunSync().head match {
      case Sample(DomainData(), RangeData(Integer(a))) =>
        assert(a == 2L)
    }
  }

  test("Project domain only") {
    val ds = DatasetGenerator("x: double -> a")
      .project("x")
    //TextWriter().write(ds)
    ds.model match {
      case Function(_: Index, x: Scalar) =>
        assert(x.id.get == id"x")
    }
    ds.samples.compile.toList.unsafeRunSync().head match {
      case Sample(DomainData(), RangeData(Real(x))) =>
        assert(x == 0.0)
    }
  }
}
