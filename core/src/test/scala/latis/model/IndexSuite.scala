package latis.model

import org.scalatest.funsuite.AnyFunSuite

import latis.util.Identifier.IdentifierStringContext

class IndexSuite extends AnyFunSuite {

  test("index type is long") {
    assert(Index().valueType == LongValueType)
  }

  test("class") {
    assert(Index().metadata.getProperty("class").contains("latis.model.Index"))
  }

  test("rename preserves type") {
    assert(Index().rename(id"foo").isInstanceOf[Index])
  }
}
