package latis.model2

import org.scalatest.funsuite.AnyFunSuite

import latis.model.DoubleValueType
import latis.model.IntValueType
import latis.model.StringValueType
import latis.util.Identifier.IdentifierStringContext

class DataTypeSuite extends AnyFunSuite {

  val a = Scalar(id"a", IntValueType)
  val b = Scalar(id"b", DoubleValueType)
  val c = Scalar(id"c", StringValueType)
  val t = Tuple.fromElements(id"t", b, c).toTry.get
  val f = Function.from(id"f", a, t).toTry.get

  test("to string") {
    assert(f.toString == "f: a -> t: (b, c)")
  }

  test("find tuple") {
    assert(f.findVariable2(id"t").nonEmpty)
  }
}
