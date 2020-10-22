package latis.input

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.util.Identifier.IdentifierStringContext

class ColumnarAdapterSpec extends FlatSpec {

  private val config = new ColumnarAdapter.Config(
    ("delimiter", " "),
    ("columns", "0,1,2;5;3;4")
  )
  private val model = Function(
    Tuple(
      Scalar(Metadata(id"time") + ("type" -> "string") + ("units" -> "yyyy MM dd"))
    ),
    Tuple(
      Scalar(Metadata(id"myInt") + ("type"    -> "int")),
      Scalar(Metadata(id"myDouble") + ("type" -> "double")),
      Scalar(Metadata(id"myString") + ("type" -> "string"))
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
}
