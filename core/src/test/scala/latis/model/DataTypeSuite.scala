package latis.model

import munit.FunSuite

import latis.data.DomainPosition
import latis.data.NullData
import latis.data.RangePosition
import latis.data.TupleData
import latis.util.Identifier.*

class DataTypeSuite extends FunSuite {

  //TODO: property based test: generate model with "a" at random path
  //  make sure we can find it
  //  make sure we can get matching path
  //  https://www.scalatest.org/user_guide/generator_driven_property_checks

  private lazy val i = Index()
  private lazy val x = Scalar(id"x", IntValueType)
  //private lazy val y = Scalar(id"y", IntValueType)
  private lazy val z = Scalar(id"z", IntValueType)
  private lazy val a = Scalar(id"a", IntValueType)
  private lazy val b = Scalar(id"b", DoubleValueType)
  private lazy val c = Scalar(id"c", StringValueType)
  private lazy val namedTup  = Tuple.fromElements(id"t", a, b)             // t: (a, b)
    .fold(fail("failed to construct tuple", _), identity)
  //private lazy val anonTup   = Tuple.fromElements(a, b).value              // (a, b)
  private lazy val nestedTup = Tuple.fromElements(namedTup, c)             // (t: (a, b), c)
    .fold(fail("failed to construct tuple", _), identity)
  private lazy val f = Function.from(id"f", x, a)                          // f: x -> a
    .fold(fail("failed to construct function", _), identity)
  private lazy val fWithTup = Function.from(id"f", x, namedTup)            // f: x -> t: (a, b)
    .fold(fail("failed to construct function", _), identity)
  private lazy val tupWithF = Tuple.fromElements(a, fWithTup)              // (a, f: x -> t: (a, b))
    .fold(fail("failed to construct tuple", _), identity)
  private lazy val nestedF = Function.from(id"g", z, f)                    // g: z -> f: x -> a
    .fold(fail("failed to construct function", _), identity)
  private lazy val nestedFInTup = Function.from(id"g", i, tupWithF)        // g: _i -> (a, f: x -> t: (a, b))
    .fold(fail("failed to construct function", _), identity)

  test("to string") {
    assertEquals(nestedFInTup.toString, "g: _i -> (a, f: x -> t: (a, b))")
  }

  test("find tuple") {
    assert(nestedFInTup.findVariable(id"t").nonEmpty)
  }

  test("don't find") {
    assert(f.findVariable(id"nope").isEmpty)
  }

  //---- findPath ----//

  test("path to constant scalar") {
    assert(a.findPath(id"a").contains(List(RangePosition(0))))
  }

  test("path to scalar in constant tuple") {
    assert(namedTup.findPath(id"b").contains(List(RangePosition(1))))
  }

  test("path to scalar in nested tuple") {
    assert(nestedTup.findPath(id"c").contains(List(RangePosition(2))))
  }

  test("path to function in top level tuple") {
    assert(tupWithF.findPath(id"f").contains(List(RangePosition(1))))
  }

  test("no path to index") {
    assert(nestedFInTup.findVariable(id"_i").nonEmpty)
    assert(nestedFInTup.findPath(id"_i").isEmpty)
  }

  test("path to tuple") {
    // t: (a, b)
    assert(fWithTup.findPath(id"t").contains(List(RangePosition(0))))
    assert(fWithTup.findPath(id"a").contains(List(RangePosition(0))))
    assert(fWithTup.findPath(id"b").contains(List(RangePosition(1))))
  }

  test("path to nested tuple") {
    // (t: (a, b), c)
    assert(nestedTup.findPath(id"t").contains(List(RangePosition(0))))
    assert(nestedTup.findPath(id"c").contains(List(RangePosition(2))))
  }

  test("path in nested tuple with function") {
    // (z, t: (f: x -> a, b), c)
    //  0  1   1  0    0  2   3
    val model = Tuple.fromElements(id"t", f, b).flatMap {
      Tuple.fromElements(z, _, c)
    }.fold(fail("failed to construct model", _), identity)

    assert(model.findPath(id"c").contains(List(RangePosition(3))))
  }

  test("path to scalar in function domain") {
    assert(fWithTup.findPath(id"x").contains(List(DomainPosition(0))))
  }

  test("path to scalar in function range") {
    assert(fWithTup.findPath(id"b").contains(List(RangePosition(1))))
  }

  test("path to scalar in nested function") {
    assert(nestedF.findPath(id"a").contains(List(RangePosition(0),RangePosition(0))))
  }

  test("path to scalar in nested function in tuple") {
    assert(nestedFInTup.findPath(id"b").contains(List(RangePosition(1),RangePosition(1))))
  }

  //---- fillData ----//

  test("scalar fill data") {
    assert(a.fillData == NullData)
  }

  test("tuple with function fill data") {
    tupWithF.fillData match {
      case TupleData(a, f) =>
        assertEquals(a, NullData)
        assertEquals(f, NullData)
      case _ => fail("unexpected tuple")
    }
  }

  //---- exists ----//

  test("exists") {
    assert(fWithTup.exists(_.isInstanceOf[Tuple]))
  }

  test("not exists") {
    assert(! f.exists(_.isInstanceOf[Tuple]))
  }

  test("self exists") {
    assert(f.exists(_.isInstanceOf[Function]))
  }
}
