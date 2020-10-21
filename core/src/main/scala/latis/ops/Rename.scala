package latis.ops

import cats.syntax.all._

import latis.data.Data
import latis.model._
import latis.util.Identifier
import latis.util.LatisException

/**
 * Define an Operation that renames a specific variable within a Dataset.
 * This only impacts the model.
 */
case class Rename(origName: String, newName: String) extends UnaryOperation {

  def applyToModel(model: DataType): Either[LatisException, DataType] = {
    // TODO: support renaming tuples and functions
    Identifier.fromString(origName) match {
      case None => Left(LatisException(s"'$origName' is not a valid Identifier"))
      case Some(origId) => model.getVariable(origId) match {
        case None => Left(LatisException(s"Variable '$origName' not found"))
        case _ => Right(model.map { s =>
          if (s.id == Some(origId)) s.rename(newName) //TODO: not validating newName as an Identifier, yet
          else s
          //TODO: support aliases with hasName
        })
      }

    }

  }

  /**
   * Provides a no-op implementation for Rename.
   */
  def applyToData(data: Data, model: DataType): Either[LatisException, Data] =
    data.asRight
}

object Rename {

  def fromArgs(args: List[String]): Either[LatisException, Rename] = args match {
    case oldName :: newName :: Nil => Right(Rename(oldName, newName))
    case _ => Left(LatisException("Rename requires exactly two arguments"))
  }
}
