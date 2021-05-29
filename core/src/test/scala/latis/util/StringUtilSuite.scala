package latis.util

import org.scalatest.funsuite.AnyFunSuite
import StringUtil._

class StringUtilSuite extends AnyFunSuite {

  test("quote unquoted string") {
    val s = "foo" //foo
    val qs = ensureDoubleQuoted(s)
    assert(qs == """"foo"""") //"foo"
  }

  test("quote quoted string") {
    val s = """"foo""""  //"foo"
    val qs = ensureDoubleQuoted(s)
    assert(qs == """"foo"""") //"foo"
  }

  test("quote partially quoted string") {
    val s = """"foo""" //"foo
    val qs = ensureDoubleQuoted(s)
    assert(qs == """"\"foo"""") //"\"foo"
  }

  test("single quote unquoted string") {
    val s = "foo" //foo
    val qs = ensureSingleQuoted(s)
    assert(qs == "'foo'") //'foo'
  }

  test("single quote quoted string") {
    val s = "'foo'"  //'foo'
    val qs = ensureSingleQuoted(s)
    assert(qs == "'foo'") //'foo'
  }

  test("single quote partially quoted string") {
    val s = "'foo" //'foo
    val qs = ensureSingleQuoted(s)
    assert(qs == "'\\\'foo'") //'\'foo'
  }

  test("quote empty string") {
    val qs = ensureQuoted("", 'q')
    assert(qs == "qq")
  }
}
