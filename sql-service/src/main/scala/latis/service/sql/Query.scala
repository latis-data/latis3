package latis.service.sql

import latis.util.Identifier

final case class Query(
  dataset: Identifier,
  projection: List[Identifier],
  selection: List[Selection]
)
