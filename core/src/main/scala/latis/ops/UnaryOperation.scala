package latis.ops

import latis.data.Data
import latis.model.DataType
import latis.util.LatisException

/**
 * Defines an Operation that acts on a single Dataset.
 */
trait UnaryOperation extends Operation {
  /**
   * Provides a new model resulting from this Operation.
   */
  def applyToModel(model: DataType): Either[LatisException, DataType]

  /**
   * Provides new Data resulting from this Operation.
   */
  def applyToData(data: Data, model: DataType): Either[LatisException, Data]
}

object UnaryOperation {
  def makeOperation(
    name: String,
    args: List[String]
  ): Either[LatisException, UnaryOperation] = name match {
    case "convertTime"     => ConvertTime.fromArgs(args)
    case "curry"           => Curry.fromArgs(args)
    case "drop"            => Drop.fromArgs(args)
    case "dropLast"        => Right(DropLast())
    case "dropRight"       => DropRight.fromArgs(args)
    case "eval"            => Evaluation.fromArgs(args)
    case "evaluation"      => Evaluation.fromArgs(args)
    case "first"           => Right(Head())
    case "formatTime"      => FormatTime.fromArgs(args)
    case "head"            => Right(Head())
    case "last"            => Right(Last())
    case "pivot"           => Pivot.fromArgs(args)
    case "project"         => Projection.fromArgs(args)
    case "rename"          => Rename.fromArgs(args)
    case "tail"            => Right(Tail())
    case "take"            => Take.fromArgs(args)
    case "takeRight"       => TakeRight.fromArgs(args)
    case "timeTupleToTime" => TimeTupleToTime.fromArgs(args)
    case n                 => Left(LatisException(s"Unknown operation: $n"))
  }
}
