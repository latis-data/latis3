package latis.util.dap2.parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.util.Identifier.IdentifierStringContext

import ast._

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
      ce.exprs.length should be (1)
      ce.exprs.head should be (Selection(id"time", Gt, "0"))
    }

  it should "parse a selection with a full ISO 8601 time string" in
    testParse("time>=2000-01-01T00:00:00.000Z") { ce =>
      ce.exprs.length should be (1)
      ce.exprs.head should be (Selection(id"time", GtEq, "2000-01-01T00:00:00.000Z"))
    }

  it should "parse a selection with a partial ISO 8601 time string" in
    testParse("time>=2000-01-01T00:00") { ce =>
      ce.exprs.length should be (1)
      ce.exprs.head should be (Selection(id"time", GtEq, "2000-01-01T00:00"))
    }

  it should "parse a selection with only a date" in
    testParse("time<2000-01-01") { ce =>
      ce.exprs.length should be (1)
      ce.exprs.head should be (Selection(id"time", Lt, "2000-01-01"))
    }

  it should "parse expressions with leading ampersands" in
    testParse("&time>0") { ce =>
      ce.exprs.length should be (1)
      ce.exprs.head should be (Selection(id"time", Gt, "0"))
    }

  it should "parse two selections joined by an ampersand" in
    testParse("time>0&time<10") { ce =>
      ce.exprs.length should be (2)
      ce.exprs(0) should be (Selection(id"time", Gt, "0"))
      ce.exprs(1) should be (Selection(id"time", Lt, "10"))
    }

  it should "parse a single-variable projection" in
    testParse("time") { ce =>
      ce.exprs.length should be (1)
      ce.exprs.head should be (Projection(List(id"time")))
    }

  it should "parse a multi-variable projection" in
    testParse("time,value") { ce =>
      ce.exprs.length should be (1)
      ce.exprs.head should be (Projection(List(id"time", id"value")))
    }

  it should "parse a single operation" in
    testParse("&first()") { ce =>
      ce.exprs.length should be (1)
      ce.exprs.head should be (Operation("first", List()))
    }

  it should "parse multiple operations" in
    testParse("&first()&last()") { ce =>
      ce.exprs.length should be (2)
      ce.exprs(0) should be (Operation("first", List()))
      ce.exprs(1) should be (Operation("last", List()))
    }

  it should "parse a mixture of projections, selections, and operations" in
    testParse("time&time<10&last()") { ce =>
      ce.exprs.length should be (3)
      ce.exprs(0) should be (Projection(List(id"time")))
      ce.exprs(1) should be (Selection(id"time", Lt, "10"))
      ce.exprs(2) should be (Operation("last", List()))
    }
}
