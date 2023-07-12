package latis.input

import munit.FunSuite

import latis.data._
import latis.dsl.ModelParser
import latis.metadata.Metadata
import latis.model._
import latis.util.Identifier._

class ColumnarAdapterSuite extends FunSuite {

  private lazy val config = new ColumnarAdapter.Config(
    ("delimiter", " "),
    ("columns", "0,1,2;5;3;4")
  )

  private lazy val model = (for {
    d <- Scalar.fromMetadata(Metadata(id"time") + ("type" -> "string") + ("units" -> "yyyy MM dd"))
    r <- ModelParser.parse("(myInt: int, myDouble: double, myString: string)")
    f <- Function.from(d, r)
  } yield f).fold(fail("failed to create model", _), identity)

  private lazy val colAdapter = new ColumnarAdapter(model, config)

  test("parse a record given column indices") {
    val record         = "1970 1 1 1.1 A 1"
    val sample         = colAdapter.parseRecord(record)
    val expectedSample = Some(Sample(DomainData("1970 1 1"), RangeData(1, 1.1, "A")))

    assertEquals(sample, expectedSample)
  }

  test("ignore a record with too few values") {
    val record = "1970 1 1 1.1 A"
    val sample = colAdapter.parseRecord(record)

    assertEquals(sample, None)
  }
}
