package latis.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Inside.inside

import latis.data.DomainPosition
import latis.data.NullData
import latis.data.RangePosition
import latis.data.TupleData
import latis.model.DoubleValueType
import latis.model.IntValueType
import latis.model.StringValueType
import latis.util.Identifier.IdentifierStringContext

class DataTypeSuite extends AnyFunSuite {

  //TODO: property based test: generate model with "a" at random path
  //  make sure we can find it
  //  make sure we can get matching path
  //  https://www.scalatest.org/user_guide/generator_driven_property_checks

  val i = Index(id"_i")
  val x = Scalar(id"x", IntValueType)
  val y = Scalar(id"y", IntValueType)
  val z = Scalar(id"z", IntValueType)
  val a = Scalar(id"a", IntValueType)
  val b = Scalar(id"b", DoubleValueType)
  val c = Scalar(id"c", StringValueType)
  val namedTup  = Tuple.fromElements(id"t", a, b).toTry.get       // t: (a, b)
  val anonTup   = Tuple.fromElements(a, b).toTry.get              // (a, b)
  val nestedTup = Tuple.fromElements(namedTup, c).toTry.get       // (t: (a, b), c)
  val f = Function.from(id"f", x, a).toTry.get                    // f: x -> a
  val fWithTup = Function.from(id"f", x, namedTup).toTry.get      // f: x -> t: (a, b)
  val tupWithF = Tuple.fromElements(a, fWithTup).toTry.get        // (a, f: x -> t: (a, b))
  val nestedF = Function.from(id"g", z, f).toTry.get              // g: z -> f: x -> a
  val nestedFInTup = Function.from(id"g", i, tupWithF).toTry.get  // g: _i -> (a, f: x -> t: (a, b))

  test("to string") {
    assert(nestedFInTup.toString == "g: _i -> (a, f: x -> t: (a, b))")
  }

  test("find tuple") {
    assert(nestedFInTup.findVariable(id"t").nonEmpty)
  }

  test("don't find") {
    assert(f.findVariable(id"nope").isEmpty)
  }

  //---- getPath ----//

  test("path to constant scalar") {
    assert(a.getPath(id"a").contains(List(RangePosition(0))))
  }

  test("path to scalar in constant tuple") {
    assert(namedTup.getPath(id"b").contains(List(RangePosition(1))))
  }

  test("path to scalar in nested tuple") {
    assert(nestedTup.getPath(id"c").contains(List(RangePosition(2))))
  }

  test("path to function in top level tuple") {
    assert(tupWithF.getPath(id"f").contains(List(RangePosition(1))))
  }

  test("no path to index") {
    assert(nestedFInTup.findVariable(id"_i").nonEmpty)
    assert(nestedFInTup.getPath(id"_i").isEmpty)
  }

  test("no path to tuple") {
    assert(fWithTup.findVariable(id"t").nonEmpty)
    assert(fWithTup.getPath(id"t").isEmpty)
  }

  test("path to scalar in function domain") {
    assert(fWithTup.getPath(id"x").contains(List(DomainPosition(0))))
  }

  test("path to scalar in function range") {
    assert(fWithTup.getPath(id"b").contains(List(RangePosition(1))))
  }

  test("path to scalar in nested function") {
    assert(nestedF.getPath(id"a").contains(List(RangePosition(0),RangePosition(0))))
  }

  test("path to scalar in nested function in tuple") {
    assert(nestedFInTup.getPath(id"b").contains(List(RangePosition(1),RangePosition(1))))
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
