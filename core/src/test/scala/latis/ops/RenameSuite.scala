package latis.ops

import munit.FunSuite

import latis.dsl.ModelParser
import latis.util.Identifier.*

class RenameSuite extends FunSuite {

  test("reconstruct the model with a single variable's name changed") {
    val model = ModelParser.unsafeParse("(a, b) -> c -> (d, e)")
    val r = Rename(id"e", id"f").applyToModel(model).fold(e => fail(e.message), identity)
    assertEquals(r.toString, "(a, b) -> c -> (d, f)")
  }
}
