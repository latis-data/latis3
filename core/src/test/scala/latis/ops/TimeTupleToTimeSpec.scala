package latis.ops

import latis.data._
import latis.dataset.Dataset
import latis.metadata.Metadata
import latis.model._
import latis.output.TextWriter
import latis.util.StreamUtils
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class TimeTupleToTimeSpec extends FlatSpec {

  "The Daily Flux dataset's time tuple" should "be converted to a time scalar" in {
    val ds = Dataset.fromName("dailyFlux")

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Number(y), Number(m), Number(d)), RangeData(Number(f))) =>
        y should be (1945)
        m should be (1)
        d should be (1)
        f should be (10)
    }
    
    val ds2 = ds.withOperation(TimeTupleToTime())

    StreamUtils.unsafeHead(ds2.samples) match {
      case Sample(DomainData(Text(time)), RangeData(Number(f))) =>
        time should be ("1945 1 1")
        f should be (10)
    }

    //TextWriter().write(ds)
  }
}
