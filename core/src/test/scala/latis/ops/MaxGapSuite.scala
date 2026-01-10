package latis.ops

import munit.CatsEffectSuite

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.data.SampledFunction
import latis.dataset.MemoizedDataset
import latis.dsl.*
import latis.metadata.Metadata
import latis.util.Identifier.id

class MaxGapSuite extends CatsEffectSuite {

  private val md = Metadata(id"test")
  private val model = ModelParser.parse("x -> y").fold(throw _, identity)

  private def makeDataset(data: (Int, Int) *) = {
    val samples = data.map { (d, r) =>
      Sample(DomainData(d), RangeData(r))
    }
    new MemoizedDataset(md, model, SampledFunction(samples))
  }

  test("insert single sample for small gap") {
    val ds = makeDataset(
      (0, 0),
      (3, 3)
    )
    val mg = MaxGap.fromArgs(List("2", "floor"))
      .fold(_ => fail("Failed to make MaxGap"), identity)
    val samples = ds.withOperation(mg).samples.compile.toList
    samples.assertEquals(
      List(
        Sample(DomainData(0), RangeData(0)),
        Sample(DomainData(2), RangeData(0)), //range from previous sample
        Sample(DomainData(3), RangeData(3))
      )
    )
  }

  test("insert single sample with nearest interpolation") {
    val ds = makeDataset(
      (0, 0),
      (3, 3)
    )
    val mg = MaxGap.fromArgs(List("2", "near"))
      .fold(_ => fail("Failed to make MaxGap"), identity)
    val samples = ds.withOperation(mg).samples.compile.toList
    samples.assertEquals(
      List(
        Sample(DomainData(0), RangeData(0)),
        Sample(DomainData(2), RangeData(3)), //range from nearest sample
        Sample(DomainData(3), RangeData(3))
      )
    )
  }

  test("insert multiple samples for large gap") {
    val ds = makeDataset(
      (0, 0),
      (5, 5)
    )
    val mg = MaxGap.fromArgs(List("2", "floor"))
      .fold(_ => fail("Failed to make MaxGap"), identity)
    val samples = ds.withOperation(mg).samples.compile.toList
    samples.assertEquals(
      List(
        Sample(DomainData(0), RangeData(0)),
        Sample(DomainData(2), RangeData(0)),
        Sample(DomainData(4), RangeData(0)),
        Sample(DomainData(5), RangeData(5))
      )
    )
  }

  test("don't insert if gap equals max") {
    val ds = makeDataset(
      (0, 0),
      (2, 2)
    )
    val mg = MaxGap.fromArgs(List("2", "floor"))
      .fold(_ => fail("Failed to make MaxGap"), identity)
    val samples = ds.withOperation(mg).samples.compile.toList
    samples.assertEquals(
      List(
        Sample(DomainData(0), RangeData(0)),
        Sample(DomainData(2), RangeData(2))
      )
    )
  }

}
