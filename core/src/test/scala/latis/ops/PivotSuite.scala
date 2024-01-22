package latis.ops

import munit.CatsEffectSuite

import latis.data.*
import latis.dataset.Dataset
import latis.dataset.MemoizedDataset
import latis.dsl.*
import latis.metadata.Metadata
import latis.model.*
import latis.util.Identifier.*

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

  test("Pivot sparse dataset with fill") {
   sparseDataset.withOperations(List(
      Curry(),  // t -> d -> f
      Pivot(List("0", "1", "2"), List("d0", "d1", "d2"))  // t -> (d0_f, d1_f, d2_f)
    )).samples.take(1).compile.lastOrError.map {
      case Sample(_, RangeData(_, n, _)) => assertEquals(n, NullData)
      case _ => fail("Bad sample")
    }
  }

  //=== Mock Datasets ===//

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

  // (t, d) -> f
  private lazy val sparseDataset: Dataset = {
    val metadata: Metadata = Metadata(id"SparseDataset")
    val model = ModelParser.unsafeParse("(t, d) -> f")
    val data: MemoizedFunction = SeqFunction(
      Seq(
        Sample(DomainData(0, 0), RangeData(10)),
        //sample at (0, 1) is not defined
        Sample(DomainData(0, 2), RangeData(20)),
        Sample(DomainData(1, 0), RangeData(30)),
        Sample(DomainData(1, 1), RangeData(40)),
        Sample(DomainData(1, 2), RangeData(50))
      )
    )
    new MemoizedDataset(metadata, model, data)
  }
}
