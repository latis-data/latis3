package latis.ops

import munit.CatsEffectSuite

import latis.data.*
import latis.dataset.MemoizedDataset
import latis.dsl.*
import latis.metadata.Metadata
import latis.model.*
import latis.util.Identifier.id

class EvaluationSuite extends CatsEffectSuite {

  private def dataset1D = {
    val md = Metadata(id"test")
    val model = ModelParser.parse("x -> a").fold(throw _, identity)
    val samples = List(
      Sample(DomainData(1), RangeData(1)),
      Sample(DomainData(3), RangeData(3)),
    )
    new MemoizedDataset(md, model, SampledFunction(samples))
  }

  private def dataset2D = {
    val md = Metadata(id"test")
    val model = (for { // (x, y) -> a
      domain <- Tuple.fromElements(Scalar(id"x", IntValueType), Scalar(id"y", IntValueType))
      func   <- Function.from(domain, Scalar(id"a", IntValueType))
    } yield func).fold(throw _, identity)
    val samples = List(
      Sample(DomainData(1, 1), RangeData(1)),
      Sample(DomainData(1, 2), RangeData(2)),
      Sample(DomainData(2, 1), RangeData(3)),
      Sample(DomainData(2, 2), RangeData(4)),
    )
    new MemoizedDataset(md, model, SampledFunction(samples))
  }

  test("1D eval first sample") {
    val eval = Evaluation(id"x", "1")
    val samples = dataset1D.withOperation(eval).samples.compile.toList
    samples.assertEquals(
      List(Sample(DomainData(), RangeData(1)))
    )
  }

  test("2D eval first dimension first sample") {
    val eval = Evaluation(id"x", "1")
    val ds = dataset2D.withOperation(eval)
    val samples = ds.samples.compile.toList
    samples.assertEquals(
      List(
        Sample(DomainData(1), RangeData(1)),
        Sample(DomainData(2), RangeData(2))
      )
    )
  }

  test("2D eval second dimension first sample") {
    val eval = Evaluation(id"y", "1")
    val ds = dataset2D.withOperation(eval)
    val samples = ds.samples.compile.toList
    samples.assertEquals(
      List(
        Sample(DomainData(1), RangeData(1)),
        Sample(DomainData(2), RangeData(3))
      )
    )
  }

  test("2D eval first dimension last sample") {
    val eval = Evaluation(id"x", "2")
    val ds = dataset2D.withOperation(eval)
    val samples = ds.samples.compile.toList
    samples.assertEquals(
      List(
        Sample(DomainData(1), RangeData(3)),
        Sample(DomainData(2), RangeData(4))
      )
    )
  }

  test("2D eval second dimension last sample") {
    val eval = Evaluation(id"y", "2")
    val ds = dataset2D.withOperation(eval)
    val samples = ds.samples.compile.toList
    samples.assertEquals(
      List(
        Sample(DomainData(1), RangeData(2)),
        Sample(DomainData(2), RangeData(4))
      )
    )
  }

}
