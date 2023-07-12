package latis.model

import munit.FunSuite

import latis.util.Identifier.*

class TupleSuite extends FunSuite {

  private lazy val a = Scalar(id"a",  IntValueType)
  private lazy val b = Scalar(id"b",  IntValueType)
  private lazy val c = Scalar(id"c",  IntValueType)
  private lazy val d = Scalar(id"d",  IntValueType)
  private lazy val t = Tuple.fromElements(a,  b)
    .fold(fail("failed to construct tuple", _), identity)
  private lazy val nestedTuple = Tuple.fromElements(t, c)
    .fold(fail("failed to construct tuple", _), identity)
  private lazy val doubleNestedTuple = Tuple.fromElements(nestedTuple, d)
    .fold(fail("failed to construct tuple", _), identity)


  test("nested tuple length") {
    assertEquals(nestedTuple.elements.length, 2)
  }

  test("nested tuple flattened length") {
    assertEquals(nestedTuple.flatElements.length, 3)
  }

  test("double nested tuple flattened length") {
    assertEquals(doubleNestedTuple.flatElements.length, 4)
  }

  test("tuple to string") {
    assertEquals(doubleNestedTuple.toString, "(((a, b), c), d)")
  }

  test("extract elements") {
    nestedTuple match {
      case Tuple(Tuple(a: Scalar, b: Scalar), c: Scalar) =>
        assert(a.id.asString == "a")
        assert(b.id.asString == "b")
        assert(c.id.asString == "c")
      case _ => fail("unexpected model")
    }
  }

  //---- Tuple construction ----//

  test("tuple from Seq of 2") {
    val tuple = Tuple.fromSeq(Seq(a,b)).fold(fail("failed to construct tuple", _), identity)
    assertEquals(tuple.elements.length, 2)
  }

  test("tuple from Seq of 3") {
    val tuple = Tuple.fromSeq(Seq(a,b,c)).fold(fail("failed to construct tuple", _), identity)
    assertEquals(tuple.elements.length, 3)
  }

  test("tuple from Seq of 1 fails") {
    assert(Tuple.fromSeq(Seq(a)).isLeft)
  }

  test("tuple from 2 elements") {
    val tuple = Tuple.fromElements(a, b).fold(fail("failed to construct tuple", _), identity)
    assertEquals(tuple.elements.length, 2)
  }

  test("tuple from 3 elements") {
    val tuple = Tuple.fromElements(a, b, c).fold(fail("failed to construct tuple", _), identity)
    assertEquals(tuple.elements.length, 3)
  }

  test("tuple from 1 element won't compile") {
    compileErrors("Tuple.fromElements(a)")
  }

  test("tuple from Seq with id") {
    val tuple = Tuple.fromSeq(id"t", Seq(a, b)).fold(fail("failed to construct tuple", _), identity)
    assertEquals(tuple.id, Option(id"t"))
  }

  test("tuple from elements with id") {
    val tuple = Tuple.fromElements(id"t", a, b).fold(fail("failed to construct tuple", _), identity)
    assertEquals(tuple.id, Option(id"t"))
  }
}
