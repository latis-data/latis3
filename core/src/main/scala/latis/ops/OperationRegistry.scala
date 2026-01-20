package latis.ops

case class OperationRegistry private (ops: Map[String, OperationBuilder]) {
  //TODO: add method for list

  def add(name: String, builder: OperationBuilder): OperationRegistry =
    OperationRegistry(ops + (name -> builder))

  def get(name: String): Option[OperationBuilder] = ops.get(name)
}

object OperationRegistry {

  def empty: OperationRegistry = OperationRegistry(Map.empty)

  def default: OperationRegistry = {
    val ops = List(
      "contains"        -> Contains.builder,
      "convertTime"     -> ConvertTime.builder,
      "count"           -> CountAggregation.builder,
      "countBy"         -> CountBy.builder,
      "curry"           -> Curry.builder,
      "curryRight"      -> CurryRight.builder,
      "drop"            -> Drop.builder,
      "dropLast"        -> DropLast.builder,
      "dropRight"       -> DropRight.builder,
      "eval"            -> Evaluation.builder,
      "evaluation"      -> Evaluation.builder,
      "first"           -> Head.builder,
      "formatTime"      -> FormatTime.builder,
      "groupBy"         -> GroupByVariable.builder,
      "groupByBinWidth" -> GroupByBinWidth.builder,
      "head"            -> Head.builder,
      "last"            -> Last.builder,
      "maxDelta"        -> MaxDelta.builder,
      "maxGap"          -> MaxGap.builder,
      "onChange"        -> OnChange.builder,
      "pivot"           -> Pivot.builder,
      "project"         -> Projection.builder,
      "rename"          -> Rename.builder,
      "select"          -> Selection.builder,
      "stats"           -> StatsAggregation.builder,
      "stride"          -> Stride.builder,
      "sum"             -> Sum.builder,
      "tail"            -> Tail.builder,
      "take"            -> Take.builder,
      "takeRight"       -> TakeRight.builder,
      "toBase64"        -> ToBase64.builder,
      "toBinary"        -> ToBinary.builder,
      "toHex"           -> ToHex.builder,
      "transpose"       -> Transpose.builder,
      "timeTupleToTime" -> TimeTupleToTime.builder
    )
    OperationRegistry(ops.toMap)
  }
}
