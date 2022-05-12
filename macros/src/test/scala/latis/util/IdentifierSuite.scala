package latis.util

import munit.FunSuite

import latis.util.Identifier._

class IdentifierSuite extends FunSuite {

  test("compile a valid Identifier") {
    id"myString"
  }

  test("equal the original string when .asString is used") {
    val id: Identifier = id"myString"
    assertEquals(id.asString, "myString")
  }

  test("be able to contain letters, numbers (not the first character), and underscores") {
    id"myString_1"
    id"_myString"
    id"abcABC_123"
    id"_123"
    id"__"
    id"myString.another._1" //TODO: remove this once namespacing is finalized and dots are disallowed
  }

  test("do not compile an invalid Identifier") {
    assertNotEquals(compileErrors("val id = id\"my string\""), "", "id containing a space should not compile")
    assertNotEquals(compileErrors("val id = id\"123_abc\""), "", "id starting with a number should not compile")
    assertNotEquals(compileErrors("Identifier(\"myString\")"), "", "id constructed like this should not compile")
  }

}
