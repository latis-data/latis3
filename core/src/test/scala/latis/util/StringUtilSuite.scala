package latis.util

import munit.FunSuite

import StringUtils._

class StringUtilsSuite extends FunSuite {

  test("quote unquoted string") {
    val s = "foo" //foo
    val qs = ensureDoubleQuoted(s)
    assertEquals(qs, """"foo"""") //"foo"
  }

  test("quote quoted string") {
    val s = """"foo""""  //"foo"
    val qs = ensureDoubleQuoted(s)
    assertEquals(qs, """"foo"""") //"foo"
  }

  test("quote partially quoted string") {
    val s = """"foo""" //"foo
    val qs = ensureDoubleQuoted(s)
    assertEquals(qs, """"\"foo"""") //"\"foo"
  }

  test("single quote unquoted string") {
    val s = "foo" //foo
    val qs = ensureSingleQuoted(s)
    assertEquals(qs, "'foo'") //'foo'
  }

  test("single quote quoted string") {
    val s = "'foo'"  //'foo'
    val qs = ensureSingleQuoted(s)
    assertEquals(qs, "'foo'") //'foo'
  }

  test("single quote partially quoted string") {
    val s = "'foo" //'foo
    val qs = ensureSingleQuoted(s)
    assertEquals(qs, "'\\\'foo'") //'\'foo'
  }

  test("quote empty string") {
    val qs = ensureQuoted("", 'q')
    assertEquals(qs, "qq")
  }

  test("remove double quotes") {
    val s = """"foo""""
    assertEquals(removeDoubleQuotes(s), "foo")
  }

  test("remove double quotes no-op") {
    val s = "foo"
    assertEquals(removeDoubleQuotes(s), "foo")
  }
}
