package latis.model

import munit.FunSuite

import latis.util.Identifier._

class IndexSuite extends FunSuite {

  test("index type is long") {
    assertEquals(Index().valueType, LongValueType)
  }

  test("class") {
    assert(Index().metadata.getProperty("class").contains("latis.model.Index"))
  }

  test("rename preserves type") {
    assert(Index().rename(id"foo").isInstanceOf[Index])
  }
}
