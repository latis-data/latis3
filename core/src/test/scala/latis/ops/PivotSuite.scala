package latis.ops

import munit.CatsEffectSuite

import latis.data._
import latis.dataset.Dataset
import latis.dsl._
import latis.model._

class PivotSuite extends CatsEffectSuite {

  test("Pivot a dataset with nested functions") {
    val expectedFirstSample = Sample(DomainData(1), RangeData(1.1, 0.1, 1.2, 0.2))

    val pivotOp = Pivot(Seq("Fe", "Mg"), Seq("Fe", "Mg"))
    // curry the dataset first to get the nested function
    val curryPivotDs = mock2d.withOperations(Seq(Curry(), pivotOp)).unsafeForce()

    curryPivotDs.model match {
      case Function(s1: Scalar, Tuple(s2: Scalar, s3: Scalar, s4: Scalar, s5: Scalar)) =>
        assertEquals(s1.id.asString, "_1")
        assertEquals(s2.id.asString, "Fe_a")
        assertEquals(s3.id.asString, "Fe_b")
        assertEquals(s4.id.asString, "Mg_a")
        assertEquals(s5.id.asString, "Mg_b")
      case _ => fail("incorrect model")
    }

    curryPivotDs.samples.take(1).compile.lastOrError.assertEquals(expectedFirstSample)
  }

  test("Pivot on a variable of type double") {
    val expectedFirstSample = Sample(DomainData(1), RangeData(1.1, 1.2))

    val pivotOp = Pivot(Seq("0.1", "0.2"), Seq("Fe", "Mg"))
    // curry the dataset first to get the nested function
    val curryPivotDs = mock2d2.withOperations(Seq(Curry(), pivotOp)).unsafeForce()

    curryPivotDs.model match {
      case Function(s1: Scalar, Tuple(s2: Scalar, s3: Scalar)) =>
        assertEquals(s1.id.asString, "_1")
        assertEquals(s2.id.asString, "Fe_a")
        assertEquals(s3.id.asString, "Mg_a")
      case _ => fail("incorrect model")
    }

    curryPivotDs.samples.take(1).compile.lastOrError.assertEquals(expectedFirstSample)
  }

  test("Pivot a single value on a small dataset") {
    /*
     * If there is only one value being pivoted, and only one variable in the range,
     * then the resulting dataset should have a scalar in the range and not a tuple.
     * This test is intended to capture that edge case.
     */
    val expectedFirstSample = Sample(DomainData(1), RangeData(1.1))

    val pivotOp = Pivot(Seq("Fe"), Seq("Fe"))
    // curry the dataset first to get the nested function
    val curryPivotDs = small2d.withOperations(Seq(Curry(), pivotOp)).unsafeForce()

    curryPivotDs.model match {
      case Function(s1: Scalar, s2: Scalar) =>
        assertEquals(s1.id.asString, "_1")
        assertEquals(s2.id.asString, "Fe_a")
      case _ => fail("incorrect model")
    }

    curryPivotDs.samples.take(1).compile.lastOrError.assertEquals(expectedFirstSample)
  }

  // (_1, _2) -> (a, b)
  private lazy val mock2d: Dataset =
    DatasetGenerator.generate2DDataset(
      Seq(1,2,3),
      Seq("Fe", "Mg"),
      Seq(Seq(1.1, 1.2, 2.1, 2.2, 3.1, 3.2)),
      Seq(Seq(0.1, 0.2, 0.3, 0.4, 0.5, 0.6)))

  // (_1, _2) -> a
  private lazy val mock2d2: Dataset =
    DatasetGenerator.generate2DDataset(
      Seq(1,2,3),
      Seq(0.1, 0.2),
      Seq(Seq(1.1, 1.2, 2.1, 2.2, 3.1, 3.2)))

  // (_1, _2) -> a
  private lazy val small2d: Dataset =
    DatasetGenerator.generate2DDataset(
      Seq(1,2,3),
      Seq("Fe", "Mg"),
      Seq(Seq(1.1, 1.2, 2.1, 2.2, 3.1, 3.2)))
}
