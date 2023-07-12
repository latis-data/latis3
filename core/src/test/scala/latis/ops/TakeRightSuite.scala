package latis.ops

import munit.CatsEffectSuite

import latis.data._
import latis.dataset.Dataset
import latis.dsl._

class TakeRightSuite extends CatsEffectSuite {

  test("return the last n samples of a simple dataset") {
    val ds: Dataset = DatasetGenerator("a -> b")
    val dsTake      = ds.withOperation(TakeRight(2))
    val samples     = dsTake.samples.compile.toList
    samples.assertEquals(
      List(
        Sample(DomainData(1), RangeData(1)),
        Sample(DomainData(2), RangeData(2))
      )
    )
  }

  test("return the last n samples of a dataset with a nested function") {
    val ds      = DatasetGenerator("(a, b) -> c").curry(1).withOperation(TakeRight(1))
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
}
