package latis.input

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.data._
import latis.dsl.ModelParser

class SubstringAdapterSpec extends AnyFlatSpec {

  private lazy val config = new SubstringAdapter.Config(
    ("substring", "0,4;5,8;9,10;11,12")
  )

  private lazy val model = ModelParser.unsafeParse("time: string -> (myDouble: double, myString: string, myInt: int)")

  private lazy val subStrAdapter = new SubstringAdapter(model, config)

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
