package latis.input

import munit.FunSuite

import latis.data._
import latis.dsl.ModelParser

class SubstringAdapterSuite extends FunSuite {

  private lazy val config = new SubstringAdapter.Config(
    ("substring", "0,4;5,8;9,10;11,12")
  )

  private lazy val model = ModelParser.unsafeParse("time: string -> (myDouble: double, myString: string, myInt: int)")

  private lazy val subStrAdapter = new SubstringAdapter(model, config)

  test("parse a record given substring indices") {
    val record         = "1970 1.1 A 1"
    val sample         = subStrAdapter.parseRecord(record)
    val expectedSample = Some(Sample(DomainData("1970"), RangeData(1.1, "A", 1)))

    assertEquals(sample, expectedSample)
  }

  test("ignore a record with too few values") {
    val record = "1970 1.1 A"
    val sample = subStrAdapter.parseRecord(record)

    assertEquals(sample, None)
  }
}
