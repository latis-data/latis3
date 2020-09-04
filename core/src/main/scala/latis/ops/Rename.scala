package latis.ops

import latis.data.SampledFunction
import latis.model._
import latis.util.LatisException

/**
 * Define an Operation that renames a specific variable within a Dataset.
 * This only impacts the model.
 */
case class Rename(origName: String, newName: String) extends UnaryOperation {

  def applyToModel(model: DataType): DataType =
    model.map {
      case v if (v.id == origName) => v.rename(newName).asInstanceOf[Scalar]
      //TODO: support aliases with hasName
      case v => v
    }

  /**
   * Provides a no-op implementation for Rename.
   */
  def applyToData(data: SampledFunction, model: DataType): SampledFunction = data
}

object Rename {

  def fromArgs(args: List[String]): Either[LatisException, Rename] = args match {
    case oldName :: newName :: Nil => Right(Rename(oldName, newName))
    case _ => Left(LatisException("Rename requires exactly two arguments"))
  }
}
