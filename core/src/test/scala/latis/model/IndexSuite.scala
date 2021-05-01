package latis.model

import org.scalatest.funsuite.AnyFunSuite

import latis.util.Identifier.IdentifierStringContext

class IndexSuite extends AnyFunSuite {

  test("Generate unique id") {
    assert(Index().id.get.asString.matches("""_i\w{8}"""))
  }

  test("rename preserves type") {
    assert(Index().rename(id"foo").isInstanceOf[Index])
  }
}
