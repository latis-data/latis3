package latis.input

import org.scalatest.EitherValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.data._
import latis.dsl.ModelParser
import latis.metadata.Metadata
import latis.model._
import latis.util.Identifier.IdentifierStringContext

class ColumnarAdapterSpec extends AnyFlatSpec {

  private val config = new ColumnarAdapter.Config(
    ("delimiter", " "),
    ("columns", "0,1,2;5;3;4")
  )
  private val model = (for {
    d <- Scalar.fromMetadata(Metadata(id"time") + ("type" -> "string") + ("units" -> "yyyy MM dd"))
    r <- ModelParser.parse("(myInt: int, myDouble: double, myString: string)")
    f <- Function.from(d, r)
  } yield f).value

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
