package latis.util.dap2.parser

import org.junit._, Assert._
import org.scalatestplus.junit.JUnitSuite

import latis.ops.parser.ast._
import ast._

class TestConstraintParser extends JUnitSuite {

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

  @Test
  def no_constraints(): Unit =
    testParse("") { ce =>
      assertEquals(0, ce.exprs.length.toLong)
    }

  @Test
  def selection(): Unit =
    testParse("time>0") { ce =>
      assertEquals(1, ce.exprs.length.toLong)

      val expected = Selection("time", Gt, "0")
      assertEquals(expected, ce.exprs.head)
    }

  @Test
  def selection_leading_and(): Unit =
    testParse("&time>0") { ce =>
      assertEquals(1, ce.exprs.length.toLong)

      val expected = Selection("time", Gt, "0")
      assertEquals(expected, ce.exprs.head)
    }

  @Test
  def two_selections(): Unit =
    testParse("time>0&time<10") { ce =>
      assertEquals(2, ce.exprs.length.toLong)

      val expected0 = Selection("time", Gt, "0")
      val expected1 = Selection("time", Lt, "10")
      assertEquals(expected0, ce.exprs(0))
      assertEquals(expected1, ce.exprs(1))
    }

  @Test
  def projection(): Unit =
    testParse("time") { ce =>
      assertEquals(1, ce.exprs.length.toLong)

      val expected = Projection(List("time"))
      assertEquals(expected, ce.exprs.head)
    }

  @Test
  def two_projections(): Unit =
    testParse("time,value") { ce =>
      assertEquals(1, ce.exprs.length.toLong)

      val expected = Projection(List("time", "value"))
      assertEquals(expected, ce.exprs.head)
    }

  @Test
  def filter(): Unit =
    testParse("&first()") { ce =>
      assertEquals(1, ce.exprs.length.toLong)

      val expected0 = Operation("first", List())
      assertEquals(expected0, ce.exprs.head)
    }

  @Test
  def two_filters(): Unit =
    testParse("&first()&last()") { ce =>
      assertEquals(2, ce.exprs.length.toLong)

      val expected0 = Operation("first", List())
      val expected1 = Operation("last", List())
      assertEquals(expected0, ce.exprs(0))
      assertEquals(expected1, ce.exprs(1))
    }

  @Test
  def projection_selection_filter(): Unit =
    testParse("time&time<10&last()") { ce =>
      assertEquals(3, ce.exprs.length.toLong)

      val expected0 = Projection(List("time"))
      val expected1 = Selection("time", Lt, "10")
      val expected2 = Operation("last", List())
      assertEquals(expected0, ce.exprs(0))
      assertEquals(expected1, ce.exprs(1))
      assertEquals(expected2, ce.exprs(2))
    }

  @Test
  def projection_selection_projection(): Unit =
    testParse("time&time<10&last") { ce =>
      assertEquals(3, ce.exprs.length.toLong)

      val expected0 = Projection(List("time"))
      val expected1 = Selection("time", Lt, "10")
      val expected2 = Projection(List("last"))
      assertEquals(expected0, ce.exprs(0))
      assertEquals(expected1, ce.exprs(1))
      assertEquals(expected2, ce.exprs(2))
    }
}
