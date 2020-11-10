package latis.ops

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.data.SampledFunction
import latis.dataset.Dataset
import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model.Scalar
import latis.tests.DatasetGenerator
import latis.util.Identifier.IdentifierStringContext

class LastSpec extends FlatSpec {

  "The Last Operation" should "return the last sample of a simple dataset" in {
    val ds: Dataset = DatasetGenerator("a -> b")
    val dsTake      = ds.withOperation(Last())
    val samples     = dsTake.samples.compile.toList.unsafeRunSync()
    samples should be(List(Sample(DomainData(2), RangeData(2))))
  }

  it should "return the last sample of a dataset with a nested function" in {
    val ds      = DatasetGenerator("(a, b) -> c").curry(1).withOperation(Last())
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

  it should "return an empty dataset when applied to an empty dataset" in {
    val md = new MemoizedDataset(
      Metadata(id"MT"),
      Scalar(Metadata(id"id") + ("type" -> "int")),
      SampledFunction(Seq.empty),
      Seq(Last())
    )
    val samples = md.samples.compile.toList.unsafeRunSync()
    samples should be(List.empty)
  }
}
