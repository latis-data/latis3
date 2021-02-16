package latis.util.dap2.parser

import atto._, Atto._
import cats.syntax.all._

import ast._
import parsers._

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
  def parse(expr: String): Either[String, ConstraintExpression] = {
    val firstSexprP: Parser[CExpr] = phrase(subexpression | projection)
    val restSexprP: Parser[CExpr] = phrase(subexpression)

    expr.dropWhile(_ === '&').split("&").toList.filter(_.nonEmpty) match {
      case Nil       => Right(ConstraintExpression(Nil))
      case x :: Nil  =>
        firstSexprP
          .parseOnly(x)
          .either
          .map(x => ConstraintExpression(List(x)))
      case x :: rest => for {
        first <- firstSexprP.parseOnly(x).either
        other <- rest.traverse(restSexprP.parseOnly(_).either)
      } yield ConstraintExpression(first :: other)
    }
  }
}
