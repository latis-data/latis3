package latis.ops

import latis.data.SampledFunction
import latis.model.DataType

/**
 * Defines an Operation that acts on a single Dataset.
 */
trait UnaryOperation extends Operation {

  /**
   * Provides a new model resulting from this Operation.
   */
  def applyToModel(model: DataType): DataType

  /**
   * Provides new Data resulting from this Operation.
   */
  def applyToData(data: SampledFunction, model: DataType): SampledFunction

}

object UnaryOperation {

  def makeOperation(name: String, args: List[String]): Option[UnaryOperation] = name match {
    case "project" => Projection.fromArgs(args)
    case "rename" => Rename.fromArgs(args)
    case "curry" => Curry.fromArgs(args)
    case "pivot" => Pivot.fromArgs(args)
    case "evaluation" => Evaluation.fromArgs(args)
    case _ => None
  }
}
