package latis.service.sql

import latis.util.Identifier

final case class Selection(
  variable: Identifier,
  op: Selection.Op,
  value: String
)

object Selection {
  sealed trait Op
  case object Eq   extends Op
  case object Gt   extends Op
  case object GtEq extends Op
  case object Lt   extends Op
  case object LtEq extends Op
}
