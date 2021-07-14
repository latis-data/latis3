package latis.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.EitherValues._
import org.scalatest.Inside.inside
import org.scalatest.OptionValues._

import latis.util.Identifier.IdentifierStringContext

class TupleSuite extends AnyFunSuite {

  private lazy val a = Scalar(id"a",  IntValueType)
  private lazy val b = Scalar(id"b",  IntValueType)
  private lazy val c = Scalar(id"c",  IntValueType)
  private lazy val d = Scalar(id"d",  IntValueType)
  private lazy val t = Tuple.fromElements(a,  b).value
  private lazy val nestedTuple = Tuple.fromElements(t, c).value
  private lazy val doubleNestedTuple = Tuple.fromElements(nestedTuple, d).value


  test("nested tuple length") {
    assert(nestedTuple.elements.length == 2)
  }

  test("nested tuple flattened length") {
    assert(nestedTuple.flatElements.length == 3)
  }

  test("double nested tuple flattened length") {
    assert(doubleNestedTuple.flatElements.length == 4)
  }

  test("tuple to string") {
    assert(doubleNestedTuple.toString == "(((a, b), c), d)")
  }

  test("extract elements") {
    inside(nestedTuple) {
      case Tuple(Tuple(a: Scalar, b: Scalar), c: Scalar) =>
        assert(a.id.asString == "a")
        assert(b.id.asString == "b")
        assert(c.id.asString == "c")
    }
  }

  //---- Tuple construction ----//

  test("tuple from Seq of 2") {
    assert(Tuple.fromSeq(Seq(a,b)).value.elements.length == 2)
  }

  test("tuple from Seq of 3") {
    assert(Tuple.fromSeq(Seq(a,b,c)).value.elements.length == 3)
  }

  test("tuple from Seq of 1 fails") {
    assert(Tuple.fromSeq(Seq(a)).isLeft)
  }

  test("tuple from 2 elements") {
    assert(Tuple.fromElements(a, b).value.elements.length == 2)
  }

  test("tuple from 3 elements") {
    assert(Tuple.fromElements(a, b, c).value.elements.length == 3)
  }

  test("tuple from 1 element won't compile") {
    assertDoesNotCompile("Tuple.fromElements(a)")
  }

  test("tuple from Seq with id") {
    assert(Tuple.fromSeq(id"t", Seq(a, b)).value.id.value == id"t")
  }

  test("tuple from elements with id") {
    assert(Tuple.fromElements(id"t", a, b).value.id.value == id"t")
  }
}
