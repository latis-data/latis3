package latis.ops

import munit.CatsEffectSuite

import latis.data.*
import latis.dataset.Dataset
import latis.dsl.*

class TakeSuite extends CatsEffectSuite {

  test("return the first n samples of a simple dataset") {
    val ds: Dataset = DatasetGenerator("a -> b")
    val dsTake      = ds.withOperation(Take(2))
    val samples     = dsTake.samples.compile.toList
    samples.assertEquals(
      List(
        Sample(DomainData(0), RangeData(0)),
        Sample(DomainData(1), RangeData(1))
      )
    )
  }

  test("return the first n samples of a dataset with a nested function") {
    val ds      = DatasetGenerator("(a, b) -> c").curry(1).withOperation(Take(1))
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

  test("treat a negative number as 0") {
    assertEquals(Take(-1).n, 0)
  }
}
