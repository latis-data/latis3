package latis.model

import munit.FunSuite

import latis.util.Identifier.*

class FunctionSuite extends FunSuite {

  private lazy val i = Index(id"_i")
  private lazy val x = Scalar(id"x", IntValueType)
  private lazy val a = Scalar(id"a", IntValueType)
  private lazy val f = Function.from(id"f", x, a)
    .fold(fail("failed to construct function", _), identity)

  test("to string") {
    assertEquals(f.toString, "f: x -> a") //TODO: add parens?
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

  test("no duplicate".ignore) {
    assert(Function.from(x, x).isLeft)
  }

  test("extract domain and range") {
    f match {
      case Function(d: Scalar, r: Scalar) =>
        assertEquals(d.id, id"x")
        assertEquals(r.id, id"a")
      case _ => fail("unexpected function")
    }
  }
}
