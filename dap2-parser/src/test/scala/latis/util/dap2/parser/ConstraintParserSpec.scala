package latis.util.dap2.parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.util.Identifier._
import latis.util.dap2.parser.ast._

class ConstraintParserSpec extends AnyFlatSpec {

  /**
   * Helper for reducing boilerplate.
   *
   * If the parser fails, the test will fail with the message given by
   * the parser. Otherwise, the assertion will be run with the result
   * of the parse as the input.
   *
   * @param expr expression to parse
   * @param assertion assertion for a successful parse
   */
  private def testParse(expr: String)(assertion: ConstraintExpression => Unit): Unit =
    ConstraintParser.parse(expr).fold(fail(_), assertion)

  "A DAP 2 constraint parser" should "accept no constraints" in
    testParse("") {
      _.exprs.length should be (0)
    }

  it should "parse a selection" in
    testParse("time>0") { ce =>
      val correct = ConstraintExpression(
        List(Selection(id"time", Gt, "0"))
      )
      ce should be (correct)
    }

  it should "parse a selection with white space" in
    testParse("time > 0") { ce =>
      val correct = ConstraintExpression(
        List(Selection(id"time", Gt, "0"))
      )
      ce should be (correct)
    }

  it should "parse a selection with a full ISO 8601 time string" in
    testParse("time>=2000-01-01T00:00:00.000Z") { ce =>
      val correct = ConstraintExpression(
        List(Selection(id"time", GtEq, "2000-01-01T00:00:00.000Z"))
      )
      ce should be (correct)
    }

  it should "parse a selection with a partial ISO 8601 time string" in
    testParse("time>=2000-01-01T00:00") { ce =>
      val correct = ConstraintExpression(
        List(Selection(id"time", GtEq, "2000-01-01T00:00"))
      )
      ce should be (correct)
    }

  it should "parse a selection with only a date" in
    testParse("time<2000-01-01") { ce =>
      val correct = ConstraintExpression(
        List(Selection(id"time", Lt, "2000-01-01"))
      )
      ce should be (correct)
    }

  it should "parse expressions with leading ampersands" in
    testParse("&time>0") { ce =>
      val correct = ConstraintExpression(
        List(Selection(id"time", Gt, "0"))
      )
      ce should be (correct)
    }

  it should "parse two selections joined by an ampersand" in
    testParse("time>0&time<10") { ce =>
      val correct = ConstraintExpression(
        List(
          Selection(id"time", Gt, "0"),
          Selection(id"time", Lt, "10")
        )
      )
      ce should be (correct)
    }

  it should "parse selections with text values" in
    testParse("&time>text") { ce =>
      val correct = ConstraintExpression(
        List(Selection(id"time", Gt, "text"))
      )
      ce should be (correct)
    }

  it should "parse selections with double-quoted values" in {
    testParse("&time>\"text\"") { ce =>
      val correct = ConstraintExpression(
        List(Selection(id"time", Gt, "\"text\""))
      )
      ce should be (correct)
    }
  }

  it should "parse a single-variable projection" in
    testParse("time") { ce =>
      val correct = ConstraintExpression(
        List(Projection(List(id"time")))
      )
      ce should be (correct)
    }

  it should "parse a multi-variable projection" in
    testParse("time,value") { ce =>
      val correct = ConstraintExpression(
        List(Projection(List(id"time", id"value")))
      )
      ce should be (correct)
    }

  it should "parse a single operation" in
    testParse("&first()") { ce =>
      val correct = ConstraintExpression(
        List(Operation("first", List()))
      )
      ce should be (correct)
    }

  it should "parse multiple operations" in
    testParse("&first()&last()") { ce =>
      val correct = ConstraintExpression(
        List(
          Operation("first", List()),
          Operation("last", List())
        )
      )
      ce should be (correct)
    }

  it should "parse an operation with an argument" in
    testParse("&take(5)") { ce =>
      val correct = ConstraintExpression(
        List(Operation("take", List("5")))
      )
      ce should be (correct)
    }

  it should "parse an operation with a double-quoted argument" in {
    testParse("&op(\"a\")") { ce =>
      val correct = ConstraintExpression(
        List(Operation("op", List("\"a\"")))
      )
      ce should be (correct)
    }
  }

  it should "parse an operation with multiple arguments" in
    testParse("&op(a,b)") { ce =>
      val correct = ConstraintExpression(
        List(Operation("op", List("a", "b")))
      )
      ce should be (correct)
    }

  it should "parse an operation with multiple arguments with white space" in
    testParse("&op( a, b )") { ce =>
      val correct = ConstraintExpression(
        List(Operation("op", List("a", "b")))
      )
      ce should be (correct)
    }

  it should "parse a projection followed by selections and operations" in
    testParse("time&time<10&last()") { ce =>
      val correct = ConstraintExpression(
        List(
          Projection(List(id"time")),
          Selection(id"time", Lt, "10"),
          Operation("last", List())
        )
      )
      ce should be (correct)
    }

  it should "not parse expressions where projections aren't first" in
    ConstraintParser.parse("time>10&time").fold(_ => succeed, _ => fail())
}
