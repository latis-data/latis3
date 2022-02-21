package latis.ops

import cats.syntax.all._

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
    case "contains" => Contains.fromArgs(args)
    case "convertTime" => ConvertTime.fromArgs(args)
    case "count" => CountAggregation().asRight
    case "countBy" => CountBy.fromArgs(args)
    case "curry" => Curry.fromArgs(args)
    case "drop" => Drop.fromArgs(args)
    case "dropLast" => DropLast().asRight
    case "dropRight" => DropRight.fromArgs(args)
    case "eval" => Evaluation.fromArgs(args)
    case "evaluation" => Evaluation.fromArgs(args)
    case "first" => Head().asRight
    case "formatTime" => FormatTime.fromArgs(args)
    case "head" => Head().asRight
    case "last" => Last().asRight
    case "pivot" => Pivot.fromArgs(args)
    case "project" => Projection.fromArgs(args)
    case "rename" => Rename.fromArgs(args)
    case "tail" => Tail().asRight
    case "take" => Take.fromArgs(args)
    case "takeRight" => TakeRight.fromArgs(args)
    case "transpose" => Transpose().asRight
    case "timeTupleToTime" => TimeTupleToTime.fromArgs(args)
    case n => Left(LatisException(s"Unknown operation: $n"))
  }
}
