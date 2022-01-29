package latis.model

import org.scalatest.EitherValues._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Inside.inside

import latis.data.Data._

class ValueTypeSuite extends AnyFunSuite {

  test("value type equality") {
    val svt = ValueType.fromName("string").value
    assert(svt == StringValueType)
    assert(svt != IntValueType)
  }

  //-- BooleanValueType --//

  test("'true' is true") {
    inside (BooleanValueType.parseValue("true")) {
      case Right(bv: BooleanValue) => assert(bv.value)
    }
  }

  test("mixed case 'tRuE' is true") {
    inside (BooleanValueType.parseValue("tRuE")) {
      case Right(bv: BooleanValue) => assert(bv.value)
    }
  }

  test("'t' is not a valid boolean value") {
    assert(BooleanValueType.parseValue("t").isLeft)
  }

  test("'false' is false") {
    inside (BooleanValueType.parseValue("false")) {
      case Right(bv: BooleanValue) => assert(! bv.value)
    }
  }

  test("mixed case 'fAlSe' is false") {
    inside (BooleanValueType.parseValue("fAlSe")) {
      case Right(bv: BooleanValue) => assert(! bv.value)
    }
  }

  test("'f' is not a valid boolean value") {
    assert(BooleanValueType.parseValue("f").isLeft)
  }
}
