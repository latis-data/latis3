package latis

import latis.metadata.Metadata
import latis.util.NetUtils.resolveUri
import latis.model._
import java.net.URI

import latis.data.{DomainData, Integer, Number, RangeData, Real, Sample, Text}
import latis.dataset.{AdaptedDataset, Dataset}
import latis.ops.Selection
import latis.util.StreamUtils
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class AnomalyDataSpec extends FlatSpec {

  "The anomalous sine wave dataset" should "be read correctly" in {
    val ds = Dataset.fromName("sine_wave_with_anomalies")
      .withOperation(Selection("time", ">=" , "2000-01-01"))
    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Number(t)),RangeData(Real(f))) =>
        t should be (1)
        f should be (0.841470985)
    }
  }
}
