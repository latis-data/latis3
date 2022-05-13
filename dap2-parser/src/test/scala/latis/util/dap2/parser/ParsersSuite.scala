package latis.util.dap2.parser

import atto.Atto._
import atto.ParseResult
import atto.Parser
import munit.FunSuite

class ParsersSuite extends FunSuite {

  test("parse expressions to operations") {
    val testOperation = testParser(parsers.operation)(_, _)
    testOperation("rename(Constantinople, Istanbul)", ast.Operation("rename", List("Constantinople", "Istanbul")))
    testOperation("curry(1)", ast.Operation("curry", List("1")))
    testOperation("pivot((1, 2), (Fe, Mg))", ast.Operation("pivot", List("(1,2)", "(Fe,Mg)")))
    testOperation("foo((1, 2), ((a, b), (c)))", ast.Operation("foo", List("(1,2)", "((a,b),(c))")))
  }

  /**
   * Partially apply with a parser to get a function that takes the string you
   * want to parse and the thing you expect to get back
   */
  private def testParser[A](p: Parser[A])(s: String, d: A): Any = p.parseOnly(s) match {
    case ParseResult.Done(_, result) => assertEquals(result, d)
    case ParseResult.Fail(_, _, m) => fail(s"$m in $s")
    // parseOnly will never return anything but Done or Fail, but the types don't
    // know that so we get a warning without the following line.
    case _ => fail(s"failed to parse $s")
  }
}
