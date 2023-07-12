package latis.input

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.data._
import latis.dsl.ModelParser

class RegexAdapterSpec extends AnyFlatSpec {

  private lazy val model = ModelParser.unsafeParse("time: string -> (myInt: int, myDouble: double, myString: string)")

  "A RegexAdapter" should "parse a record given a pattern" in {
    val config = new RegexAdapter.Config(
      ("delimiter", " "),
      ("pattern", """(\S+)\s+(\S+)\s+(\S+)\s+(\S+)""")
    )
    val regexAdapter   = new RegexAdapter(model, config)
    val record         = "1970/01/01  1 1.1 A"
    val sample         = regexAdapter.parseRecord(record)
    val expectedSample = Some(Sample(DomainData("1970/01/01"), RangeData(1, 1.1, "A")))

    sample should be(expectedSample)
  }

  "A RegexAdapter" should "ignore a record that doesn't match the given pattern" in {
    val config = new RegexAdapter.Config(
      ("delimiter", " "),
      ("pattern", """(2000.*)\s+(\S+)\s+(\S+)\s+(\S+)""")
    )
    val regexAdapter = new RegexAdapter(model, config)
    val record       = "1970/01/01  1 1.1 A"
    val sample       = regexAdapter.parseRecord(record)

    sample should be(None)
  }

  "A RegexAdapter" should "parse a record given a pattern and column indices" in {
    val config = new RegexAdapter.Config(
      ("delimiter", " "),
      ("pattern", """(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)"""),
      ("columns", "0,1,2;5;3;4")
    )
    val regexAdapter   = new RegexAdapter(model, config)
    val record         = "1970 1 1 1.1 A 1"
    val sample         = regexAdapter.parseRecord(record)
    val expectedSample = Some(Sample(DomainData("1970 1 1"), RangeData(1, 1.1, "A")))

    sample should be(expectedSample)
  }

  "A RegexAdapter" should "ignore a record with too few values" in {
    val config = new RegexAdapter.Config(
      ("delimiter", " "),
      ("pattern", """(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)"""),
      ("columns", "0,1,2;5;3;4")
    )
    val regexAdapter = new RegexAdapter(model, config)
    val record       = "1970 1 1 1.1 A"
    val sample       = regexAdapter.parseRecord(record)

    sample should be(None)
  }
}