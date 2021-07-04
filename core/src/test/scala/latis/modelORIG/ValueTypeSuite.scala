package latis.modelORIG

import org.scalatest.funsuite.AnyFunSuite

class ValueTypeSuite extends AnyFunSuite {

  test("value type equality") {
    val svt = ValueType.fromName("string").toTry.get
    assert(svt == StringValueType)
    assert(svt != IntValueType)
  }
}
