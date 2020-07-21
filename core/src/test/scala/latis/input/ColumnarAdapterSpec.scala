package latis.input

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import latis.data._
import latis.dataset.Dataset
import latis.metadata.Metadata
import latis.model._
import latis.ops._
import latis.output.TextWriter
import latis.util.StreamUtils

class ColumnarAdapterSpec extends FlatSpec {

  private val config = new ColumnarAdapter.Config(
    ("delimiter", " "),
    ("columns", "0,1,2;5;3;4")
  )
  private val model = Function(
    Tuple(
      Scalar(Metadata("time") + ("type" -> "string") + ("units" -> "yyyy MM dd"))
    ),
    Tuple(
      Scalar(Metadata("myInt") + ("type"    -> "int")),
      Scalar(Metadata("myDouble") + ("type" -> "double")),
      Scalar(Metadata("myString") + ("type" -> "string"))
    )
  )
  private val colAdapter = new ColumnarAdapter(model, config)

  "A ColumnarAdapter" should "parse a record given column indices" in {
    val record         = "1970 1 1 1.1 A 1"
    val sample         = colAdapter.parseRecord(record)
    val expectedSample = Some(Sample(DomainData("1970 1 1"), RangeData(1, 1.1, "A")))

    sample should be(expectedSample)
  }

  "A ColumnarAdapter" should "ignore a record with too few values" in {
    val record = "1970 1 1 1.1 A"
    val sample = colAdapter.parseRecord(record)

    sample should be(None)
  }
  
  "The Daily Flux dataset" should "be read from an FDML file" in {
    val ds = Dataset.fromName("dailyFlux")
      .withOperation(TimeTupleToTime())
    
    TextWriter().write(ds)
    
//    StreamUtils.unsafeHead(ds.samples) match {
//      case Sample(DomainData(Number(t)),RangeData(Real(f))) =>
//        t should be (1)
//        f should be (0.841470985)
//    }
    
  }
  
}
