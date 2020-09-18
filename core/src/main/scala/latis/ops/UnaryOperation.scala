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
    case "curry" => Curry.fromArgs(args)
    case "eval" => Evaluation.fromArgs(args)
    case "evaluation" => Evaluation.fromArgs(args)
    case "pivot" => Pivot.fromArgs(args)
    case "project" => Projection.fromArgs(args)
    case "rename" => Rename.fromArgs(args)
    case "timeTupleToTime" => TimeTupleToTime.fromArgs(args)
    case n => Left(LatisException(s"Unknown operator: $n"))
  }
}
