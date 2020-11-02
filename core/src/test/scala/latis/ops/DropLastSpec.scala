package latis.ops

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.dataset.Dataset
import latis.util.DatasetGenerator

class DropLastSpec extends FlatSpec {

  "The DropLast Operation" should "drop the last sample from a dataset" in {
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
}
