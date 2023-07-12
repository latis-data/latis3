package latis.input

import munit.FunSuite

import latis.data.*
import latis.dsl.ModelParser

class FormattedTextAdapterSuite extends FunSuite {

  private lazy val model = ModelParser.unsafeParse("time: string -> (myInt: int, myDouble: double, myString: string)")

  test("parse a record given a format") {
    val config = FormattedTextAdapter.Config(
      ("delimiter", " "),
      ("format", """(I4,2I2)/(I1)(F3.1)(A)""")
    )
    val formatAdapter  = new FormattedTextAdapter(model, config)
    val record         = "19700101 11.1A"
    val sample         = formatAdapter.parseRecord(record)
    val expectedSample = Some(Sample(DomainData("19700101"), RangeData(1, 1.1, "A")))

    assertEquals(sample, expectedSample)
  }

  test("ignore a record if the format doesn't match") {
    val config = FormattedTextAdapter.Config(
      ("delimiter", " "),
      ("format", """(I4,2I2)(I1)/(F3.1)(A)""")
    )
    val formatAdapter  = new FormattedTextAdapter(model, config)
    val record         = "19700101 11.1A"
    val sample         = formatAdapter.parseRecord(record)
    val expectedSample = None

    assertEquals(sample, expectedSample)
  }

}
