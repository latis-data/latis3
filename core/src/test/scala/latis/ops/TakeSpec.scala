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
import latis.model.ModelParser
import latis.util.DatasetGenerator

class TakeSpec extends FlatSpec {

  "The Take Operation" should "return the first n samples of a simple dataset" in {
    val ds: Dataset = DatasetGenerator("a -> b")
    val dsTake     = ds.withOperation(Take(2))
    val samples     = dsTake.samples.compile.toList.unsafeRunSync()
    samples should be(
      List(
        Sample(DomainData(0), RangeData(0)),
        Sample(DomainData(1), RangeData(1))
      )
    )
  }

  it should "return the first n samples of a dataset with a nested function" in {
    val ds = DatasetGenerator("a -> b")
    val mf = DatasetGenerator("c -> d")
    val nestedSamples = ds.data.sampleSeq.map {
      case (d, _) => Sample(d, Seq(mf.data))
    }
    val nestedDs = new MemoizedDataset(
      Metadata("nestedDataset"),
      ModelParser("a -> c -> d").fold(throw _, identity),
      SampledFunction(nestedSamples),
      Seq(Take(2))
    )
    val samples = nestedDs.samples.compile.toList.unsafeRunSync()
    val expectedNestedFunction = SampledFunction(
      Seq(
        Sample(DomainData(0), RangeData(0)),
        Sample(DomainData(1), RangeData(1)),
        Sample(DomainData(2), RangeData(2))
      )
    )

    samples should be(
      List(
        Sample(DomainData(0), Seq(expectedNestedFunction)),
        Sample(DomainData(1), Seq(expectedNestedFunction))
      )
    )
  }
}
