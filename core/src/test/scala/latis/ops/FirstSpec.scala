package latis.ops

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data.DomainData
import latis.data.MemoizedFunction
import latis.data.RangeData
import latis.data.Sample
import latis.data.SampledFunction
import latis.dataset.Dataset
import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model.ModelParser
import latis.model.Scalar
import latis.util.DatasetGenerator

class FirstSpec extends FlatSpec {

  "The First Operation" should "return the first sample of a simple dataset" in {
    val ds: Dataset = DatasetGenerator("a -> b")
    val dsFirst     = ds.withOperation(First())
    val samples     = dsFirst.samples.compile.toList.unsafeRunSync()
    samples should be(List(Sample(DomainData(0), RangeData(0))))
  }

  it should "return the first sample of a dataset with a nested function" in {
    val ds = DatasetGenerator("(a, b) -> c").curry(1).first()
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

  it should "return an empty dataset when applied to an empty dataset" in {
    val md = new MemoizedDataset(
      Metadata("MT"),
      Scalar(Metadata("id") + ("type" -> "int")),
      SampledFunction(Seq.empty),
      Seq(First())
    )
    val samples = md.samples.compile.toList.unsafeRunSync()
    samples should be(List.empty)
  }
}
