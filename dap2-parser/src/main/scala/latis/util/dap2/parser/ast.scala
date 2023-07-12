package latis.util.dap2.parser

import latis.util.Identifier

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
      case Projection(n)       => n.map(_.asString).mkString(",")
      case Selection(n, op, v) => s"${n.asString}${prettyOp(op)}$v"
      case Operation(n, args)  => s"$n(${args.mkString(",")})"
    }.mkString("&")
  }

  sealed trait CExpr

  final case class Projection(
    names: List[Identifier]
  ) extends CExpr

  final case class Selection(
    name: Identifier, op: SelectionOp, value: String
  ) extends CExpr

  final case class Operation(
    name: String, args: List[String]
  ) extends CExpr

  sealed trait SelectionOp
  case object Gt extends SelectionOp
  case object Lt extends SelectionOp
  case object Eq extends SelectionOp
  case object GtEq extends SelectionOp
  case object LtEq extends SelectionOp
  case object EqEq extends SelectionOp
  case object NeEq extends SelectionOp
  case object Tilde extends SelectionOp
  case object EqTilde extends SelectionOp
  case object NeEqTilde extends SelectionOp

  /**
   * Pretty print a selection operator.
   *
   * @param op selection operator to pretty print
   */
  def prettyOp(op: SelectionOp): String = op match {
    case Gt        => ">"
    case Lt        => "<"
    case Eq        => "="
    case GtEq      => ">="
    case LtEq      => "<="
    case EqEq      => "=="
    case NeEq      => "!="
    case Tilde     => "~"
    case EqTilde   => "=~"
    case NeEqTilde => "!=~"
  }
}
