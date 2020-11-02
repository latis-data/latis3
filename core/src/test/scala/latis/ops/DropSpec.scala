package latis.ops

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.dataset.Dataset
import latis.util.DatasetGenerator

class DropSpec extends FlatSpec {

  "The Drop Operation" should "drop the first n samples of a dataset" in {
    val ds: Dataset = DatasetGenerator("a -> b")
    val dsDrop     = ds.withOperation(Drop(1))
    val samples     = dsDrop.samples.compile.toList.unsafeRunSync()
    samples should be(
      List(
        Sample(DomainData(1), RangeData(1)),
        Sample(DomainData(2), RangeData(2))
      )
    )
  }
}
