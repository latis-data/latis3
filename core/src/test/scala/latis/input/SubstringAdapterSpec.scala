package latis.input

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data._
import latis.metadata.Metadata
import latis.model._

class SubstringAdapterSpec extends FlatSpec {

  private val config = new SubstringAdapter.Config(
    ("substring", "0,4;5,8;9,10;11,12")
  )
  private val model = Function(
    Tuple(
      Scalar(Metadata("time") + ("type" -> "string") + ("units" -> "yyyy"))
    ),
    Tuple(
      Scalar(Metadata("myDouble") + ("type" -> "double")),
      Scalar(Metadata("myString") + ("type" -> "string")),
      Scalar(Metadata("myInt") + ("type"    -> "int"))
    )
  )
  private val subStrAdapter = new SubstringAdapter(model, config)

  "A SubstringAdapter" should "parse a record given substring indices" in {
    val record         = "1970 1.1 A 1"
    val sample         = subStrAdapter.parseRecord(record)
    val expectedSample = Some(Sample(DomainData("1970"), RangeData(1.1, "A", 1)))

    sample should be(expectedSample)
  }

  "A SubstringAdapter" should "ignore a record with too few values" in {
    val record = "1970 1.1 A"
    val sample = subStrAdapter.parseRecord(record)

    sample should be(None)
  }
}
