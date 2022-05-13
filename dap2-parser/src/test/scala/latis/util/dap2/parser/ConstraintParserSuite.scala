package latis.util.dap2.parser

import munit.CatsEffectSuite

import latis.util.Identifier._
import latis.util.dap2.parser.ast._

class ConstraintParserSuite extends CatsEffectSuite {

  /**
   * Helper for reducing boilerplate.
   *
   * If the parser fails, the test will fail with the message given by
   * the parser. Otherwise, the assertion will be run with the result
   * of the parse as the)put.
   *
   * @param expr expression to parse
   * @param assertion assertion for a successful parse
   */
  private def testParse(expr: String)(assertion: ConstraintExpression => Any): Any =
    ConstraintParser.parse(expr).fold(fail(_), assertion)

  test("accept no constraints") {
    testParse("") { ce =>
      assertEquals(ce.exprs.length, 0)
    }
  }

  test("parse a selection") {
    testParse("time>0") { ce =>
      val correct = ConstraintExpression(
        List(Selection(id"time", Gt, "0"))
      )
      assertEquals(ce, correct)
    }
  }

  test("parse a selection with white space") {
    testParse("time > 0") { ce =>
      val correct = ConstraintExpression(
        List(Selection(id"time", Gt, "0"))
      )
      assertEquals(ce, correct)
    }
  }

  test("parse a selection with a full ISO 8601 time string") {
    testParse("time>=2000-01-01T00:00:00.000Z") { ce =>
      val correct = ConstraintExpression(
        List(Selection(id"time", GtEq, "2000-01-01T00:00:00.000Z"))
      )
      assertEquals(ce, correct)
    }
  }

  test("parse a selection with a partial ISO 8601 time string") {
    testParse("time>=2000-01-01T00:00") { ce =>
      val correct = ConstraintExpression(
        List(Selection(id"time", GtEq, "2000-01-01T00:00"))
      )
      assertEquals(ce, correct)
    }
  }

  test("parse a selection with only a date") {
    testParse("time<2000-01-01") { ce =>
      val correct = ConstraintExpression(
        List(Selection(id"time", Lt, "2000-01-01"))
      )
      assertEquals(ce, correct)
    }
  }

  test("parse expressions with leading ampersands") {
    testParse("&time>0") { ce =>
      val correct = ConstraintExpression(
        List(Selection(id"time", Gt, "0"))
      )
      assertEquals(ce, correct)
    }
  }

  test("parse two selections joined by an ampersand") {
    testParse("time>0&time<10") { ce =>
      val correct = ConstraintExpression(
        List(
          Selection(id"time", Gt, "0"),
          Selection(id"time", Lt, "10")
        )
      )
      assertEquals(ce, correct)
    }
  }

  test("parse selections with text values") {
    testParse("&time>text") { ce =>
      val correct = ConstraintExpression(
        List(Selection(id"time", Gt, "text"))
      )
      assertEquals(ce, correct)
    }
  }

  test("parse selections with double-quoted values") {
    testParse("&time>\"text\"") { ce =>
      val correct = ConstraintExpression(
        List(Selection(id"time", Gt, "\"text\""))
      )
      assertEquals(ce, correct)
    }
  }

  test("parse a single-variable projection") {
    testParse("time") { ce =>
      val correct = ConstraintExpression(
        List(Projection(List(id"time")))
      )
      assertEquals(ce, correct)
    }
  }

  test("parse a multi-variable projection") {
    testParse("time,value") { ce =>
      val correct = ConstraintExpression(
        List(Projection(List(id"time", id"value")))
      )
      assertEquals(ce, correct)
    }
  }

  test("parse a single operation") {
    testParse("&first()") { ce =>
      val correct = ConstraintExpression(
        List(Operation("first", List()))
      )
      assertEquals(ce, correct)
    }
  }

  test("parse multiple operations") {
    testParse("&first()&last()") { ce =>
      val correct = ConstraintExpression(
        List(
          Operation("first", List()),
          Operation("last", List())
        )
      )
      assertEquals(ce, correct)
    }
  }

  test("parse an operation with an argument") {
    testParse("&take(5)") { ce =>
      val correct = ConstraintExpression(
        List(Operation("take", List("5")))
      )
      assertEquals(ce, correct)
    }
  }

  test("parse an operation with a double-quoted argument") {
    testParse("&op(\"a\")") { ce =>
      val correct = ConstraintExpression(
        List(Operation("op", List("\"a\"")))
      )
      assertEquals(ce, correct)
    }
  }

  test("parse an operation with multiple arguments") {
    testParse("&op(a,b)") { ce =>
      val correct = ConstraintExpression(
        List(Operation("op", List("a", "b")))
      )
      assertEquals(ce, correct)
    }
  }

  test("parse an operation with multiple arguments with white space") {
    testParse("&op( a, b )") { ce =>
      val correct = ConstraintExpression(
        List(Operation("op", List("a", "b")))
      )
      assertEquals(ce, correct)
    }
  }

  test("parse a projection followed by selections and operations") {
    testParse("time&time<10&last()") { ce =>
      val correct = ConstraintExpression(
        List(
          Projection(List(id"time")),
          Selection(id"time", Lt, "10"),
          Operation("last", List())
        )
      )
      assertEquals(ce, correct)
    }
  }

  test("not parse expressions where projections aren't first") {
    ConstraintParser.parse("time>10&time").fold(_ => assert(cond = true), _ => fail(""))
  }
}
