package latis.ops

import munit.CatsEffectSuite

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.data.SampledFunction
import latis.dataset.Dataset
import latis.dataset.MemoizedDataset
import latis.dsl._
import latis.metadata.Metadata
import latis.model.IntValueType
import latis.model.Scalar
import latis.util.Identifier._

class LastSuite extends CatsEffectSuite {

  test("return the last sample of a simple dataset") {
    val ds: Dataset = DatasetGenerator("a -> b")
    val dsTake      = ds.withOperation(Last())
    val samples     = dsTake.samples.compile.toList
    samples.assertEquals(List(Sample(DomainData(2), RangeData(2))))
  }

  test("return the last sample of a dataset with a nested function") {
    val ds      = DatasetGenerator("(a, b) -> c").curry(1).withOperation(Last())
    val samples = ds.samples.compile.toList
    val sf = SampledFunction(
      Seq(
        Sample(DomainData(0), RangeData(3)),
        Sample(DomainData(1), RangeData(4)),
        Sample(DomainData(2), RangeData(5))
      )
    )
    samples.assertEquals(List(Sample(DomainData(1), Seq(sf))))
  }

  test("return an empty dataset when applied to an empty dataset") {
    val md = new MemoizedDataset(
      Metadata(id"MT"),
      Scalar(id"id", IntValueType),
      SampledFunction(Seq.empty),
      Seq(Last())
    )
    val samples = md.samples.compile.toList
    samples.assertEquals(List.empty)
  }
}
