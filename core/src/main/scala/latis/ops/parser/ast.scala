package latis.ops.parser

object ast {

  sealed trait CExpr

  final case class Projection(
    names: List[String]
  ) extends CExpr

  final case class Selection(
    name: String, op: SelectionOp, value: String
  ) extends CExpr

  final case class Operation(
    name: String, args: List[String]
  ) extends CExpr

  sealed trait SelectionOp
  final case object Gt extends SelectionOp
  final case object Lt extends SelectionOp
  final case object Eq extends SelectionOp
  final case object GtEq extends SelectionOp
  final case object LtEq extends SelectionOp
  final case object EqEq extends SelectionOp
  final case object NeEq extends SelectionOp
  final case object Tilde extends SelectionOp
  final case object EqTilde extends SelectionOp
  final case object NeEqTilde extends SelectionOp

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
