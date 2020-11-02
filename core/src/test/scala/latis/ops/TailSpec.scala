package latis.ops

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.dataset.Dataset
import latis.util.DatasetGenerator

class TailSpec extends FlatSpec {

  "The Tail Operation" should "drop the first sample from a dataset" in {
    val ds: Dataset = DatasetGenerator("a -> b")
    val dsTail      = ds.withOperation(Tail())
    val samples     = dsTail.samples.compile.toList.unsafeRunSync()
    samples should be(
      List(
        Sample(DomainData(1), RangeData(1)),
        Sample(DomainData(2), RangeData(2))
      )
    )
  }
}
