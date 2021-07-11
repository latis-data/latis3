package latis.model

import org.scalatest.EitherValues._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Inside.inside

import latis.util.Identifier.IdentifierStringContext

class FunctionSuite extends AnyFunSuite {

  private val i = Index(id"_i")
  private val x = Scalar(id"x", IntValueType)
  private val a = Scalar(id"a", IntValueType)
  private val f = Function.from(id"f", x, a).value

  test("to string") {
    assert(f.toString == "f: x -> a") //TODO: add parens?
  }

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

  test("no function in domain") {
    assert(Function.from(f, a).isLeft)
  }

  test("no index in range") {
    assert(Function.from(x, i).isLeft)
  }

  ignore("no duplicate") {
    assert(Function.from(x, x).isLeft)
  }

  test("extract domain and range") {
    inside(f) {
      case Function(d: Scalar, r: Scalar) =>
        assert(d.id == id"x")
        assert(r.id == id"a")
    }
  }
}
