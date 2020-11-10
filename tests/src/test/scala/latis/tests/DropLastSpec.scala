package latis.ops

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.data.SampledFunction
import latis.dataset.Dataset
import latis.tests.DatasetGenerator

class DropLastSpec extends FlatSpec {

  "The DropLast Operation" should "drop the last sample from a simple dataset" in {
    val ds: Dataset = DatasetGenerator("a -> b")
    val dsDrop      = ds.withOperation(DropLast())
    val samples     = dsDrop.samples.compile.toList.unsafeRunSync()
    samples should be(
      List(
        Sample(DomainData(0), RangeData(0)),
        Sample(DomainData(1), RangeData(1))
      )
    )
  }

  it should "drop the last sample from a dataset with a nested function" in {
    val ds      = DatasetGenerator("(a, b) -> c").curry(1).withOperation(DropLast())
    val samples = ds.samples.compile.toList.unsafeRunSync()
    val sf = SampledFunction(
      Seq(
        Sample(DomainData(0), RangeData(0)),
        Sample(DomainData(1), RangeData(1)),
        Sample(DomainData(2), RangeData(2))
      )
    )
    samples should be(List(Sample(DomainData(0), Seq(sf))))
  }
}
