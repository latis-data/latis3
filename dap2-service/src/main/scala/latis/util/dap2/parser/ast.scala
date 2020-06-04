package latis.util.dap2.parser

import latis.ops.parser.ast._

/**
 * Module for the DAP 2 constraint expression AST.
 */
object ast {

  /**
   * Wrapper for DAP 2 constraint expressions.
   */
  final case class ConstraintExpression(exprs: List[CExpr]) extends AnyVal

  /**
   * Pretty print a constraint expression.
   *
   * @param expr constraint expression to pretty print
   */
  def pretty(expr: ConstraintExpression): String = {

    expr.exprs.map {
      case Projection(n)       => n.mkString(",")
      case Selection(n, op, v) => s"$n${prettyOp(op)}$v"
      case Operation(n, args)  => s"$n(${args.mkString(",")})"
    }.mkString("&")
  }
}
