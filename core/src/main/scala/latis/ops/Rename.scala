package latis.ops

import cats.syntax.all.*

import latis.data.Data
import latis.model.*
import latis.util.Identifier
import latis.util.LatisException

/**
 * Define an Operation that renames a specific variable within a Dataset.
 * This only impacts the model.
 */
case class Rename(origId: Identifier, newId: Identifier) extends UnaryOperation {

  def applyToModel(model: DataType): Either[LatisException, DataType] = {
    // TODO: support renaming tuples and functions
    model.findVariable(origId) match {
      case None => Left(LatisException(s"Variable '${origId.asString}' not found"))
      case _ => Right(model.map {
        //TODO: works for scalars only
        case s: Scalar if (s.id == origId) => s.rename(newId)
        case dt => dt
      })
    }
  }

  /**
   * Provides a no-op implementation for Rename.
   */
  def applyToData(data: Data, model: DataType): Either[LatisException, Data] =
    data.asRight

  override def toString = s"Rename(${origId.asString}, ${newId.asString})"
}

object Rename {

  def builder: OperationBuilder = (args: List[String]) =>
    fromArgs(args)

  def fromArgs(args: List[String]): Either[LatisException, Rename] = Either.catchOnly[LatisException] {
    args.map { arg =>
      Identifier.fromString(arg)
        .getOrElse {
          throw LatisException(s"'$arg' is not a valid identifier")
        }
    } match {
      case oldId :: newId :: Nil => Rename(oldId, newId)
      case _ => throw LatisException("Rename requires exactly two arguments")
    }
  }
}
