package latis.model

import org.scalatest.EitherValues._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Inside.inside

import latis.data.DomainPosition
import latis.data.NullData
import latis.data.RangePosition
import latis.data.TupleData
import latis.util.Identifier.IdentifierStringContext

class DataTypeSuite extends AnyFunSuite {

  //TODO: property based test: generate model with "a" at random path
  //  make sure we can find it
  //  make sure we can get matching path
  //  https://www.scalatest.org/user_guide/generator_driven_property_checks

  private val i = Index(id"_i")
  private val x = Scalar(id"x", IntValueType)
  //private val y = Scalar(id"y", IntValueType)
  private val z = Scalar(id"z", IntValueType)
  private val a = Scalar(id"a", IntValueType)
  private val b = Scalar(id"b", DoubleValueType)
  private val c = Scalar(id"c", StringValueType)
  private val namedTup  = Tuple.fromElements(id"t", a, b).value       // t: (a, b)
  //private val anonTup   = Tuple.fromElements(a, b).value              // (a, b)
  private val nestedTup = Tuple.fromElements(namedTup, c).value       // (t: (a, b), c)
  private val f = Function.from(id"f", x, a).value                    // f: x -> a
  private val fWithTup = Function.from(id"f", x, namedTup).value      // f: x -> t: (a, b)
  private val tupWithF = Tuple.fromElements(a, fWithTup).value        // (a, f: x -> t: (a, b))
  private val nestedF = Function.from(id"g", z, f).value              // g: z -> f: x -> a
  private val nestedFInTup = Function.from(id"g", i, tupWithF).value  // g: _i -> (a, f: x -> t: (a, b))

  test("to string") {
    assert(nestedFInTup.toString == "g: _i -> (a, f: x -> t: (a, b))")
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
    val model = Tuple.fromElements(z, Tuple.fromElements(id"t", f, b).value, c).value
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
    inside(tupWithF.fillData ) {
      case TupleData(a, f) =>
        assert(a == NullData)
        assert(f == NullData)
    }
  }

}
