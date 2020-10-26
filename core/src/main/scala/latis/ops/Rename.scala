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
case class Rename(origName: Identifier, newName: Identifier) extends UnaryOperation {

  def applyToModel(model: DataType): Either[LatisException, DataType] = {
    // TODO: support renaming tuples and functions (findVariable instead of getVariable?)
    model.getVariable(origName) match {
      case None => Left(LatisException(s"Variable '${origName.asString}' not found"))
      case _ => Right(model.map { s =>
        if (s.id.contains(origName)) s.rename(newName)
        else s
        //TODO: support aliases with hasName
      })
    }
  }

  /**
   * Provides a no-op implementation for Rename.
   */
  def applyToData(data: Data, model: DataType): Either[LatisException, Data] =
    data.asRight
}

object Rename {

  def fromArgs(args: List[String]): Either[LatisException, Rename] = Either.catchOnly[LatisException] {
    args.map(arg => Identifier.fromString(arg)
      .getOrElse {
        throw LatisException(s"'$arg' is not a valid identifier")
      }
    ) match {
      case oldName :: newName :: Nil => Rename(oldName, newName)
      case _ => throw LatisException("Rename requires exactly two arguments")
    }
  }
}
