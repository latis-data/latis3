package latis.service.sql
package parser

import latis.util.Identifier.*

final class SqlParserSuite extends munit.FunSuite {

  test("parse a query without a projection or selection") {
    val expected = Query(id"dataset", List.empty, List.empty)

    SqlParser.parse("select * from dataset").fold(
      err => fail(s"$err"),
      query => assertEquals(query, expected)
    )
  }

  test("parse a query with a projection") {
    val expected = Query(id"dataset", List(id"a", id"b", id"c"), List.empty)

    SqlParser.parse("select a, b, c from dataset").fold(
      err => fail(s"$err"),
      query => assertEquals(query, expected)
    )
  }

  test("parse a query with a selection") {
    val expected = Query(id"dataset", List.empty, List(
      Selection(id"a", Selection.Lt, "1"), Selection(id"b", Selection.Gt, "2")
    ))

    SqlParser.parse("select * from dataset where a < 1 and b > 2").fold(
      err => fail(s"$err"),
      query => assertEquals(query, expected)
    )
  }

  test("parse a query on multiple lines") {
    val expected = Query(id"dataset", List.empty, List(
      Selection(id"a", Selection.Lt, "1"), Selection(id"b", Selection.Gt, "2")
    ))

    SqlParser.parse("select * from dataset\r\nwhere a < 1 and b > 2").fold(
      err => fail(s"$err"),
      query => assertEquals(query, expected)
    )
  }

  test("handle leading whitespace") {
    val expected = Query(id"dataset", List.empty, List.empty)

    SqlParser.parse(" \t \n \r\nselect * from dataset").fold(
      err => fail(s"$err"),
      query => assertEquals(query, expected)
    )
  }

  test("handle trailing whitespace") {
    val expected = Query(id"dataset", List.empty, List.empty)

    SqlParser.parse("select * from dataset \t \n \r\n").fold(
      err => fail(s"$err"),
      query => assertEquals(query, expected)
    )
  }
}
