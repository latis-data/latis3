package latis.model2

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Inside.inside

import latis.model.IntValueType
import latis.util.Identifier.IdentifierStringContext

class FunctionSuite extends AnyFunSuite {

  val i = Index(id"_i")
  val x = Scalar(id"x", IntValueType)
  val a = Scalar(id"a", IntValueType)
  val f = Function.from(id"f", x, a).toTry.get

  test("function with id") {
    Function.from(id"f", x, a).map { f =>
      assert(f.id.contains(id"f"))
    }
  }

  test("function without id") {
    Function.from(x, a).map { f =>
      assert(f.id.isEmpty)
    }
  }

  test("extract domain and range") {
    inside(f) {
      case Function(d: Scalar, r: Scalar) =>
        assert(d.id == id"x")
        assert(r.id == id"a")
    }
  }

  ignore("no duplicate") {
    assert(Function.from(x, x).isLeft)
  }

  ignore("no function in domain") {
    assert(Function.from(f, a).isLeft)
  }

  ignore("no index in range") {
    assert(Function.from(x, i).isLeft)
  }

}
