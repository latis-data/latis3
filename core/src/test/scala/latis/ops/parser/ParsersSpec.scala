package latis.ops.parser

import atto.Atto._
import atto.Parser
import atto.ParseResult
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data.Data

class ParsersSpec extends FlatSpec {

  "parsers.data" should "parse to Data of the correct type" in {
    def testData = testParser(parsers.data)(_, _)
    testData("1", Data.IntValue(1))
    testData("1l", Data.LongValue(1L))
    testData("1.1f", Data.FloatValue(1.1f))
    testData("1.1", Data.DoubleValue(1.1))
    testData("1.1e4", Data.DoubleValue(1.1e4))
    testData("true", Data.BooleanValue(true))
    testData("foo", Data.StringValue("foo"))
  }

  "parsers.operation" should "parse expressions to operations" in {
    def testOperation = testParser(parsers.operation)(_, _)
    testOperation("rename(Constantinople, Istanbul)", ast.Operation("rename", List("Constantinople", "Istanbul")))
    testOperation("curry(1)", ast.Operation("curry", List("1")))
    testOperation("pivot((1, 2), (Fe, Mg))", ast.Operation("pivot", List("(1,2)", "(Fe,Mg)")))
    testOperation("foo((1, 2), ((a, b), (c)))", ast.Operation("foo", List("(1,2)", "((a,b),(c))")))
  }

  /**
   * partially apply with a parser to get a function that takes the string you
   * want to parse and the thing you expect to get back
   */
  private def testParser[A](p: Parser[A])(s: String, d: A): Unit = p.parseOnly(s) match {
    case ParseResult.Done(_, result) => result should be (d)
    case ParseResult.Fail(_, _, m) => throw new Exception(s"$m in $s")
    // parseOnly will never return anything but Done or Fail, but the types don't
    // know that so we get a warning without the following line.
    case _ => throw new Exception(s"failed to parse $s")
  }
}
