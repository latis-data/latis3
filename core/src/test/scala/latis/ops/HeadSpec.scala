package latis.ops

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.data.SampledFunction
import latis.dataset.Dataset
import latis.dataset.MemoizedDataset
import latis.dsl._
import latis.metadata.Metadata
import latis.model.Scalar
import latis.util.DatasetGenerator
import latis.util.Identifier.IdentifierStringContext

class HeadSpec extends AnyFlatSpec {

  "The Head Operation" should "return the first sample of a simple dataset" in {
    val ds: Dataset = DatasetGenerator("a -> b")
    val dsFirst     = ds.withOperation(Head())
    val samples     = dsFirst.samples.compile.toList.unsafeRunSync()
    samples should be(List(Sample(DomainData(0), RangeData(0))))
  }

  it should "return the first sample of a dataset with a nested function" in {
    val ds      = DatasetGenerator("(a, b) -> c").curry(1).head()
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
      Metadata(id"MT"),
      Scalar(Metadata(id"id") + ("type" -> "int")),
      SampledFunction(Seq.empty),
      Seq(Head())
    )
    val samples = md.samples.compile.toList.unsafeRunSync()
    samples should be(List.empty)
  }
}
