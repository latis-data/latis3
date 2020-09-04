package latis.ops

import latis.data.SampledFunction
import latis.model._
import latis.util.LatisException

/**
 * Define an Operation that renames a specific variable within a Dataset.
 * This only impacts the model.
 */
case class Rename(origName: String, newName: String) extends UnaryOperation {

  def applyToModel(model: DataType): Either[LatisException, DataType] =
  // TODO: support renaming tuples and functions
    model.getVariable(origName) match {
      case None => Left(LatisException(s"Variable '$origName' not found"))
      case _ => Right(model.map { s =>
        if (s.id == origName) s.rename(newName)
        else s
        //TODO: support aliases with hasName
      })
    }

  /**
   * Provides a no-op implementation for Rename.
   */
  def applyToData(data: SampledFunction, model: DataType): Either[LatisException, SampledFunction] = Right(data)
}

object Rename {

  def fromArgs(args: List[String]): Either[LatisException, Rename] = args match {
    case oldName :: newName :: Nil => Right(Rename(oldName, newName))
    case _ => Left(LatisException("Rename requires exactly two arguments"))
  }
}
