package latis.input

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.util.Identifier.IdentifierStringContext

class RegexAdapterSpec extends AnyFlatSpec {

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
