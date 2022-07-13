package latis.ops

import munit.CatsEffectSuite

import latis.data._
import latis.dsl._
import latis.model._
import latis.util.Identifier._

class ProjectionSuite extends CatsEffectSuite {

  test("Project two from Tuple") {
    val model = ModelParser.unsafeParse("(a, b, c)")

    Projection.fromExpression("a,b").flatMap(_.applyToModel(model)) match {
      case Right(Tuple(a: Scalar, b: Scalar)) =>
        assertEquals(a.id, id"a")
        assertEquals(b.id, id"b")
      case _ => fail("incorrect model")
    }
  }

  test("Project one from Tuple") {
    //Note: Tuple reduced to Scalar
    val model = ModelParser.unsafeParse("(a, b, c)")
      Projection.fromExpression("b").flatMap(_.applyToModel(model)) match {
        case Right(b: Scalar) =>
          assertEquals(b.id, id"b")
        case _ => fail("incorrect model")
      }
  }

  test("Reduce nested tuple") {
    val model = ModelParser.unsafeParse("(a, (b, c))")
    Projection.fromExpression("a,b").flatMap(_.applyToModel(model)) match {
      case Right(Tuple(a: Scalar, b: Scalar)) =>
        assertEquals(a.id, id"a")
        assertEquals(b.id, id"b")
      case _ => fail("incorrect model")
    }
  }

  // I don't think ModelParser supports names tuples.
  test("Project named nested tuple".ignore) {
    val model = ModelParser.unsafeParse("(a, t:(b, c))")
    Projection.fromExpression("t").flatMap(_.applyToModel(model)) match {
      case Right(t: Tuple) =>
        assertEquals(t.id, Option(id"t"))
      case _ => fail("incorrect model")
    }
  }

  test("Project one range variable") {
    val ds = DatasetGenerator("x -> (a, b: string)")
      .project("x,b")
    ds.model match {
      case Function(d: Scalar, r: Scalar) =>
        assertEquals(d.id, id"x")
        assertEquals(r.id, id"b") //Note: tuple reduced to scalar
      case _ => fail("incorrect model")
    }

    ds.samples.take(1).compile.lastOrError.assertEquals(
      Sample(List(0), List("a"))
    )
  }

  test("Replace unprojected domain variable with Index") {
    val ds = DatasetGenerator("x: double -> a")
      .project("a")
    ds.model match {
      case Function(_: Index, a: Scalar) =>
        assertEquals(a.id, id"a")
      case _ => fail("incorrect model")
    }

    ds.samples.take(1).compile.lastOrError.assertEquals(
      Sample(List.empty, List(0))
    )
  }

  test("Replace unprojected 2D domain variable with single Index") {
    val ds = DatasetGenerator("(x, y) -> a")
      .project("a")
      .drop(2)
    ds.model match {
      case Function(i: Index, a: Scalar) =>
        assertEquals(i.id, id"_ix_y")
        assertEquals(a.id, id"a")
      case _ => fail("incorrect model")
    }

    ds.samples.take(1).compile.lastOrError.assertEquals(
      Sample(List.empty, List(2))
    )
  }

  test("Project domain only") {
    val ds = DatasetGenerator("x: double -> a")
      .project("x")
    ds.model match {
      case Function(_: Index, x: Scalar) =>
        assertEquals(x.id, id"x")
      case _ => fail("incorrect model")
    }

    ds.samples.take(1).compile.lastOrError.assertEquals(
      Sample(List.empty, List(0.0))
    )
  }
}
