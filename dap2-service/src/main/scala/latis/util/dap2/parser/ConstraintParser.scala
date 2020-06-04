package latis.util.dap2.parser

import atto._, Atto._
import cats.implicits._

import latis.ops.parser.ast.CExpr
import latis.ops.parser.parsers._

/**
 * Module for parsing DAP 2 constraint expressions.
 *
 * This replicates what was done in LaTiS 2 rather than strictly
 * follow the DAP 2 specification.
 */
object ConstraintParser {

  /**
   * Parse a DAP 2 constraint expression into a sequence of LaTiS
   * operations.
   *
   * @param expr non-URL-encoded DAP 2 constraint expression
   */
  def parse(expr: String): Either[String, ast.ConstraintExpression] = {
    def p(sexpr: String): Either[String, CExpr] =
      (phrase(subexpression) | err(s"Invalid subexpression: $sexpr"))
        .parseOnly(sexpr).either

    if (expr.nonEmpty) {
      val sexprs = expr.dropWhile(_ === '&').split("&").toList
      sexprs.traverse(p).map(ast.ConstraintExpression)
    } else {
      Right(ast.ConstraintExpression(List.empty))
    }
  }

}
