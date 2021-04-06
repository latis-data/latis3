package latis.data

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class DataSpec extends AnyFlatSpec {
  "Identical Data" should "be equivalent" in {
    val d1: Data = 1.1
    val d2: Data = 1.1
    d1 should be(d2)
  }

  "Identical DomainData" should "be equivalent" in {
    val dd1 = DomainData(1.1, 1.1f)
    val dd2 = DomainData(1.1, 1.1f)
    dd1 should be(dd2)
  }

  "Double Data values" should "not equal Float Data values" in {
    val d: Double = 1.0d
    val f: Float  = 1.0f
    val dd: Data  = 1.0f
    val df: Data  = 1.0d

    //Note, Doubles can equals Floats (but not always, e.g. 1.1)
    d should be(f)
    dd should not be (df)
  }

  "Implicitly constructed Boolean Data" should "be extractable through pattern matching" in {
    val tup = TupleData(true, false)
    tup match {
      case TupleData(BooleanDatum(t), BooleanDatum(f)) =>
        t should be(true)
        f should be(false)
    }
  }

  "BooleanValues" should "be represented properly as Strings" in {
    val tup = TupleData(true, false)
    tup match {
      case TupleData(t: Data.BooleanValue, f: Data.BooleanValue) =>
        t.asString should be("true")
        f.asString should be("false")
    }
  }
}
