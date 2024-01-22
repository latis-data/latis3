package latis.model

import munit.FunSuite

import latis.data.Data.*

class ValueTypeSuite extends FunSuite {

  test("value type equality") {
    val svt = ValueType.fromName("string").fold(fail("failed to construct ValueType", _), identity)

    assertEquals(svt, StringValueType)
    assertNotEquals(svt, IntValueType.asInstanceOf[ValueType])
  }

  //-- BooleanValueType --//

  test("'true' is true") {
    BooleanValueType.parseValue("true") match {
      case Right(bv: BooleanValue) => assert(bv.value)
      case _ => fail("")
    }
  }

  test("mixed case 'tRuE' is true") {
    BooleanValueType.parseValue("tRuE") match {
      case Right(bv: BooleanValue) => assert(bv.value)
      case _ => fail("")
    }
  }

  test("'t' is not a valid boolean value") {
    assert(BooleanValueType.parseValue("t").isLeft)
  }

  test("'false' is false") {
    BooleanValueType.parseValue("false") match {
      case Right(bv: BooleanValue) => assert(! bv.value)
      case _ => fail("")
    }
  }

  test("mixed case 'fAlSe' is false") {
    BooleanValueType.parseValue("fAlSe") match {
      case Right(bv: BooleanValue) => assert(! bv.value)
      case _ => fail("")
    }
  }

  test("'f' is not a valid boolean value") {
    assert(BooleanValueType.parseValue("f").isLeft)
  }
}
