package latis.ops

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.data.SampledFunction
import latis.dataset.Dataset
import latis.util.DatasetGenerator

class TakeRightSpec extends FlatSpec {

  "The TakeRight Operation" should "return the last n samples of a simple dataset" in {
    val ds: Dataset = DatasetGenerator("a -> b")
    val dsTake      = ds.withOperation(TakeRight(2))
    val samples     = dsTake.samples.compile.toList.unsafeRunSync()
    samples should be(
      List(
        Sample(DomainData(1), RangeData(1)),
        Sample(DomainData(2), RangeData(2))
      )
    )
  }

  it should "return the last n samples of a dataset with a nested function" in {
    val ds = DatasetGenerator("(a, b) -> c").curry(1).withOperation(TakeRight(1))
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
