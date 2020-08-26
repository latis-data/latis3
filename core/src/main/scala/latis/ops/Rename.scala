package latis.ops

import latis.data.SampledFunction
import latis.model._

/**
 * Define an Operation that renames a specific variable within a Dataset.
 * This only impacts the model.
 */
case class Rename(origName: String, newName: String) extends UnaryOperation {

  def applyToModel(model: DataType): DataType =
    model.map {
      case v if (v.id == origName) => v.rename(newName)
      //TODO: support aliases with hasName
      case v => v
    }

  /**
   * Provides a no-op implementation for Rename.
   */
  def applyToData(data: SampledFunction, model: DataType): SampledFunction = data
}

object Rename {

  def fromArgs(args: List[String]): Option[UnaryOperation] = args match {
    case oldName :: newName :: Nil => Some(Rename(oldName, newName))
    case _ => None
  }
}
