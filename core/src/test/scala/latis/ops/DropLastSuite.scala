package latis.ops

import munit.CatsEffectSuite

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.data.SampledFunction
import latis.dataset.Dataset
import latis.dsl._

class DropLastSuite extends CatsEffectSuite {

  test("drop the last sample from a simple dataset") {
    val ds: Dataset = DatasetGenerator("a -> b")
    val dsDrop      = ds.withOperation(DropLast())
    val samples     = dsDrop.samples.compile.toList
    samples.assertEquals(
      List(
        Sample(DomainData(0), RangeData(0)),
        Sample(DomainData(1), RangeData(1))
      )
    )
  }

  test("drop the last sample from a dataset with a nested function") {
    val ds      = DatasetGenerator("(a, b) -> c").curry(1).withOperation(DropLast())
    val samples = ds.samples.compile.toList
    val sf = SampledFunction(
      Seq(
        Sample(DomainData(0), RangeData(0)),
        Sample(DomainData(1), RangeData(1)),
        Sample(DomainData(2), RangeData(2))
      )
    )
    samples.assertEquals(List(Sample(DomainData(0), Seq(sf))))
  }
}
