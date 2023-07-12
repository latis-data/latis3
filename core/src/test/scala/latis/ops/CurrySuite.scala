package latis.ops

import munit.CatsEffectSuite

import latis.data._
import latis.dataset.Dataset
import latis.dataset.MemoizedDataset
import latis.dsl._
import latis.model._

class CurrySuite extends CatsEffectSuite {

  test("Curry a 2D dataset") {
    val curriedDs = mock2d.withOperation(Curry()).unsafeForce()

    assertEquals(curriedDs.model.toString, "_1 -> _2 -> (a, b)")

    curriedDs.model match {
      case Function(s1: Scalar, Function(s2: Scalar, Tuple(s3: Scalar, s4: Scalar))) =>
        assertEquals(s1.id.asString, "_1")
        assertEquals(s2.id.asString, "_2")
        assertEquals(s3.id.asString, "a")
        assertEquals(s4.id.asString, "b")
      case _ => fail("incorrect model")
    }

    curriedDs.samples.take(1).compile.lastOrError.flatMap {
      case Sample(DomainData(Number(t)), RangeData(f)) =>
        assertEquals(t, 1.0)

        f.samples.compile.toList.assertEquals(
          List(
            Sample(List("Fe"), List(1.1, 0.1)),
            Sample(List("Mg"), List(1.2, 0.2))
          )
        )
      case _ => fail("incorrect sample")
    }
  }

  test("Curry a 2D dataset to arity 2 (no change)") {
    val curriedDs = mock2d.withOperation(Curry(2)).unsafeForce()

    assertEquals(curriedDs.model.toString, "(_1, _2) -> (a, b)")

    curriedDs.model match {
      case Function(Tuple(s1: Scalar, s2: Scalar), Tuple(s3: Scalar, s4: Scalar)) =>
        assertEquals(s1.id.asString, "_1")
        assertEquals(s2.id.asString, "_2")
        assertEquals(s3.id.asString, "a")
        assertEquals(s4.id.asString, "b")
      case _ => fail("incorrect model")
    }

    curriedDs.samples.take(1).compile.lastOrError.assertEquals(
      Sample(List(Data.IntValue(1), Data.StringValue("Fe")), List(Data.DoubleValue(1.1), Data.DoubleValue(0.1)))
    )
  }

  test("Curry a 3D dataset to arity 2") {
    // should be "(x, y) -> z -> flux" after curry
    val curriedDs = mock3d.withOperation(Curry(2)).unsafeForce()

    assertEquals(curriedDs.model.toString, "(_1, _2) -> _3 -> a")

    curriedDs.model match {
      case Function(Tuple(s1: Scalar, s2: Scalar), Function(s3: Scalar, s4: Scalar)) =>
        assertEquals(s1.id.asString, "_1")
        assertEquals(s2.id.asString, "_2")
        assertEquals(s3.id.asString, "_3")
        assertEquals(s4.id.asString, "a")
      case _ => fail("incorrect model")
    }

    curriedDs.samples.take(1).compile.lastOrError.flatMap {
      case Sample(DomainData(Integer(d1), Integer(d2)), RangeData(f)) =>
        assertEquals(d1, 1L)
        assertEquals(d2, 1L)

        f.samples.compile.toList.assertEquals(
          List(
            Sample(List(1), List(10.0)),
            Sample(List(2), List(20.0))
          )
        )
      case _ => fail("incorrect sample")
    }
  }

  // (_1, _2) -> (a, b)
  private lazy val mock2d: Dataset =
    DatasetGenerator.generate2DDataset(
      Seq(1,2,3),
      Seq("Fe", "Mg"),
      Seq(Seq(1.1, 1.2, 2.1, 2.2, 3.1, 3.2)),
      Seq(Seq(0.1, 0.2, 0.3, 0.4, 0.5, 0.6)))

  // (_1, _2, _3) -> a
  private lazy val mock3d: MemoizedDataset =
    DatasetGenerator.generate3DDataset(
      Seq(1,2),
      Seq(1,2),
      Seq(1,2),
      Seq(Seq(Seq(10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0))))
}
