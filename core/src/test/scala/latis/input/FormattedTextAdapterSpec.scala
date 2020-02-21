package latis.input

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data._
import latis.metadata.Metadata
import latis.model._

class FormattedTextAdapterSpec extends FlatSpec {

  private val model = Function(
    Tuple(
      Scalar(Metadata("time") + ("type" -> "string") + ("units" -> "yyyyMMdd"))
    ),
    Tuple(
      Scalar(Metadata("myInt") + ("type"    -> "int")),
      Scalar(Metadata("myDouble") + ("type" -> "double")),
      Scalar(Metadata("myString") + ("type" -> "string"))
    )
  )

  "A FormattedTextAdapter" should "parse a record given a format" in {
    val config = FormattedTextAdapter.Config(
      ("delimiter", " "),
      ("format", """(I4,2I2)/(I1)(F3.1)(A)""")
    )
    val formatAdapter  = new FormattedTextAdapter(model, config)
    val record         = "19700101 11.1A"
    val sample         = formatAdapter.parseRecord(record)
    val expectedSample = Some(Sample(DomainData("19700101"), RangeData(1, 1.1, "A")))

    sample should be(expectedSample)
  }

  "A FormattedTextAdapter" should "ignore a record if the format doesn't match" in {
    val config = FormattedTextAdapter.Config(
      ("delimiter", " "),
      ("format", """(I4,2I2)(I1)/(F3.1)(A)""")
    )
    val formatAdapter  = new FormattedTextAdapter(model, config)
    val record         = "19700101 11.1A"
    val sample         = formatAdapter.parseRecord(record)
    val expectedSample = None

    sample should be(expectedSample)
  }

}
