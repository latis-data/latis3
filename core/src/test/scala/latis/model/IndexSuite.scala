package latis.model

import org.scalatest.funsuite.AnyFunSuite

import latis.util.Identifier.IdentifierStringContext

class IndexSuite extends AnyFunSuite {

  test("rename preserves type") {
    assert(Index(id"i").rename(id"foo").isInstanceOf[Index])
  }
}
