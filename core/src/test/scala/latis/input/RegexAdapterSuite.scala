package latis.input

import munit.FunSuite

import latis.data.*
import latis.dsl.ModelParser

class RegexAdapterSuite extends FunSuite {

  private lazy val model = ModelParser.unsafeParse("time: string -> (myInt: int, myDouble: double, myString: string)")

  test("parse a record given a pattern") {
    val config = new RegexAdapter.Config(
      ("delimiter", " "),
      ("pattern", """(\S+)\s+(\S+)\s+(\S+)\s+(\S+)""")
    )
    val regexAdapter   = new RegexAdapter(model, config)
    val record         = "1970/01/01  1 1.1 A"
    val sample         = regexAdapter.parseRecord(record)
    val expectedSample = Some(Sample(DomainData("1970/01/01"), RangeData(1, 1.1, "A")))

    assertEquals(sample, expectedSample)
  }

  test("ignore a record that doesn't match the given pattern") {
    val config = new RegexAdapter.Config(
      ("delimiter", " "),
      ("pattern", """(2000.*)\s+(\S+)\s+(\S+)\s+(\S+)""")
    )
    val regexAdapter = new RegexAdapter(model, config)
    val record       = "1970/01/01  1 1.1 A"
    val sample       = regexAdapter.parseRecord(record)

    assertEquals(sample, None)
  }

  test("parse a record given a pattern and column indices") {
    val config = new RegexAdapter.Config(
      ("delimiter", " "),
      ("pattern", """(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)"""),
      ("columns", "0,1,2;5;3;4")
    )
    val regexAdapter   = new RegexAdapter(model, config)
    val record         = "1970 1 1 1.1 A 1"
    val sample         = regexAdapter.parseRecord(record)
    val expectedSample = Some(Sample(DomainData("1970 1 1"), RangeData(1, 1.1, "A")))

    assertEquals(sample, expectedSample)
  }

  test("ignore a record with too few values") {
    val config = new RegexAdapter.Config(
      ("delimiter", " "),
      ("pattern", """(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)"""),
      ("columns", "0,1,2;5;3;4")
    )
    val regexAdapter = new RegexAdapter(model, config)
    val record       = "1970 1 1 1.1 A"
    val sample       = regexAdapter.parseRecord(record)

    assertEquals(sample, None)
  }
}
