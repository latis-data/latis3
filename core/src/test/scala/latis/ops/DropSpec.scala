package latis.ops

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.data.SampledFunction
import latis.dataset.Dataset
import latis.util.DatasetGenerator

class DropSpec extends AnyFlatSpec {
  "The Drop Operation" should "drop the first n samples of a simple dataset" in {
    val ds: Dataset = DatasetGenerator("a -> b")
    val dsDrop      = ds.withOperation(Drop(1))
    val samples     = dsDrop.samples.compile.toList.unsafeRunSync()
    samples should be(
      List(
        Sample(DomainData(1), RangeData(1)),
        Sample(DomainData(2), RangeData(2))
      )
    )
  }

  it should "drop the first n samples of a dataset with a nested function" in {
    val ds      = DatasetGenerator("(a, b) -> c").curry(1).withOperation(Drop(1))
    val samples = ds.samples.compile.toList.unsafeRunSync()
    val sf = SampledFunction(
      Seq(
        Sample(DomainData(0), RangeData(3)),
        Sample(DomainData(1), RangeData(4)),
        Sample(DomainData(2), RangeData(5))
      )
    )
    samples should be(List(Sample(DomainData(1), Seq(sf))))
  }
}
