package latis.modelORIG

import org.scalatest.EitherValues._
import org.scalatest.funsuite.AnyFunSuite

class ValueTypeSuite extends AnyFunSuite {

  test("value type equality") {
    val svt = ValueType.fromName("string").value
    assert(svt == StringValueType)
    assert(svt != IntValueType)
  }
}
