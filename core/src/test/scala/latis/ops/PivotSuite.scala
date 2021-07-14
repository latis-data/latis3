package latis.ops

import cats.effect.unsafe.implicits.global
import org.scalatest.Inside._
import org.scalatest.funsuite.AnyFunSuite

import latis.data._
import latis.dataset.Dataset
import latis.dsl._
import latis.model._

class PivotSuite extends AnyFunSuite {

  test("Pivot a dataset with nested functions") {
    val expectedFirstSample = Sample(DomainData(1), RangeData(1.1, 0.1, 1.2, 0.2))

    val pivotOp = Pivot(Seq("Fe", "Mg"), Seq("Fe", "Mg"))
    // curry the dataset first to get the nested function
    val curryPivotDs = mock2d.withOperations(Seq(Curry(), pivotOp)).unsafeForce()
    val samples = curryPivotDs.samples.compile.toList.unsafeRunSync()

    assert(samples.head == expectedFirstSample)

    inside(curryPivotDs.model) { case Function(domain, range) =>
      inside(domain) { case s: Scalar => assert(s.id.asString == "_1") }
      inside(range) { case Tuple(r1, r2, r3, r4) =>
        inside(r1) { case s: Scalar => assert(s.id.asString == "Fe_a") }
        inside(r2) { case s: Scalar => assert(s.id.asString == "Fe_b") }
        inside(r3) { case s: Scalar => assert(s.id.asString == "Mg_a") }
        inside(r4) { case s: Scalar => assert(s.id.asString == "Mg_b") }
      }
    }
  }

  test("Pivot on a variable of type double") {
    val expectedFirstSample = Sample(DomainData(1), RangeData(1.1, 1.2))

    val pivotOp = Pivot(Seq("0.1", "0.2"), Seq("Fe", "Mg"))
    // curry the dataset first to get the nested function
    val curryPivotDs = mock2d2.withOperations(Seq(Curry(), pivotOp)).unsafeForce()
    val samples = curryPivotDs.samples.compile.toList.unsafeRunSync()

    assert(samples.head == expectedFirstSample)

    inside(curryPivotDs.model) { case Function(domain, range) =>
      inside(domain) { case s: Scalar => assert(s.id.asString == "_1") }
      inside(range) { case Tuple(r1, r2) =>
        inside(r1) { case s: Scalar => assert(s.id.asString == "Fe_a") }
        inside(r2) { case s: Scalar => assert(s.id.asString == "Mg_a") }
      }
    }
  }

  test("Pivot a single value on a small dataset") {
    /**
     * If there is only one value being pivoted, and only one variable in the range,
     * then the resulting dataset should have a scalar in the range and not a tuple.
     * This test is intended to capture that edge case.
     */
    val expectedFirstSample = Sample(DomainData(1), RangeData(1.1))

    val pivotOp = Pivot(Seq("Fe"), Seq("Fe"))
    // curry the dataset first to get the nested function
    val curryPivotDs = small2d.withOperations(Seq(Curry(), pivotOp)).unsafeForce()
    val samples = curryPivotDs.samples.compile.toList.unsafeRunSync()

    assert(samples.head == expectedFirstSample)

    inside(curryPivotDs.model) { case Function(domain, range) =>
      inside(domain) { case s: Scalar => assert(s.id.asString == "_1") }
      inside(range)  { case s: Scalar => assert(s.id.asString == "Fe_a") }
    }
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
