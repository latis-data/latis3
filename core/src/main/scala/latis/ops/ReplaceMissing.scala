package latis.ops

import cats.syntax.all.*

import latis.data.*
import latis.model.*
import latis.util.Identifier
import latis.util.LatisException


/**
 * Operation to replace missing values specified in a Dataset.
 * 
 * The ReplaceMissing operation replaces values that match the `missingValue` in the model, or
 * if not defined, values that match the `fillValue`. The metadata is updated with `missingValue` 
 * set to the new value and `fillValue`, if defined, is removed. Assumes a simple function, i.e.
 * the operation does not work with nested functions.
 * 
 * @params id The Identifier of the Scalar variable to be updated
 * @params replacement A string of the new value to replace missing values
 */
case class ReplaceMissing(id: Identifier, replacement: String) extends MapOperation {
   /**
   * Checks if data in a sample should be replaced if it matches the value for missing data. 
   * Checks for the value given in metadata for `missingValue`, and if not defined, the value 
   * given in metadata for `fillValue`. Also checks if `missingValue` is NaN.
   * 
   * @params scalar The Scalar variable with the target id
   * @params data Data from a Sample
   */
  private def isMissing(scalar: Scalar, data: Data): Boolean = {
    scalar.missingValue
      .orElse(scalar.fillValue) // look for fillValue if missingValue is not defined
      .map { missingVal =>
        missingVal match {
          case Real(v) if v.isNaN() =>
            data match {
              case Real(d) => d.isNaN()
              case _ => false
            }
          case _ =>
            (data == missingVal)
        }
      }
      .getOrElse(false) // no-op
  }

  def mapFunction(model: DataType): Sample => Sample = {
    val position = model.findPath(id) match {
      case Some(head :: Nil) =>
        head
      case Some(head :: tail) =>
        val msg = s"Variable with id $id found in nested function, unable to replace"
        throw LatisException(msg)
      case _ =>
        val msg = s"Couldn't find variable with id: $id"
        throw LatisException(msg)
    }

    val scalar = model.findVariable(id) match {
      case Some(s: Scalar) => s
      case _ => throw LatisException(s"Scalar variable not found: ${id.asString}")
    }

    (sample: Sample) =>
      sample.getValue(position) match {
        case Some(d) =>
          val newScalar = scalar.convertValue(replacement).fold(throw _, identity)

          if (isMissing(scalar, d)) {
            sample.updatedValue(position, newScalar)
          } else {
            sample
          }
        case _ => sample
      }
  }
  
  override def applyToModel(model: DataType): Either[LatisException, DataType] = {
    // The metadata should be updated with missingValue set to the new value and fillValue should be removed.
    Either.catchOnly[LatisException](model.map {
      case s: Scalar if (s.id == id) =>
        s.missingValue.orElse(s.fillValue) match {
          case Some(_) =>
            val md = s.metadata + ("missingValue", replacement) - ("fillValue")
            Scalar.fromMetadata(md).fold(throw _, identity)
          case None => s // no-op
        }
      case dt => dt
    })
  }
}

object ReplaceMissing {

  def builder: OperationBuilder = (args: List[String]) => fromArgs(args)

  def fromArgs(args: List[String]): Either[LatisException, ReplaceMissing] = args match {
    case name :: fill :: Nil =>
      for {
        id <- Identifier.fromString(name)
          .toRight(LatisException("ReplaceMissing identifier invalid"))
      } yield ReplaceMissing(id, fill)
    case _ =>
      val msg = "ReplaceMissing requires a variable id and fill value arguments"
      LatisException(msg).asLeft
  }
}
