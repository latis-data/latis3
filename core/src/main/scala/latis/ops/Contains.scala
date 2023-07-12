package latis.ops

import cats.syntax.all._

import latis.data._
import latis.model._
import latis.util.Identifier
import latis.util.LatisException

/**
 * Defines an Operation to keep only Samples with a variable
 * that matches one of the given values.
 */
case class Contains(id: Identifier, values: String*) extends Filter {
  //TODO: support nested functions, aliases,... (See Selection)

  def predicate(model: DataType): Either[LatisException, Sample => Boolean] = {
    // Determine the position of the selected variable
    def getPosition: Either[LatisException, SamplePosition] =
      model.findPath(id)
        .toRight(LatisException(s"Variable not found: ${id.asString}"))
        .flatMap { path =>
          path.length match {
            case 1 => path.head.asRight
            case _ =>
              val msg = "Contains does not support values in nested Functions."
              LatisException(msg).asLeft
          }
        }

    // Get the target Scalar variable
    def getScalar: Either[LatisException, Scalar] = model.findVariable(id) match {
      case Some(s: Scalar) => s.asRight
      case _ => LatisException(s"Scalar variable not found: ${id.asString}").asLeft
    }

    // Convert values to appropriate type for comparison
    def getComparisonValues(scalar: Scalar): Either[LatisException, Seq[Datum]] =
      values.traverse { v =>
        scalar.convertValue(v).leftMap { le =>
          LatisException(s"Invalid comparison value: $v", le)
        }
      }

    for {
      pos    <- getPosition
      scalar <- getScalar
      cvals  <- getComparisonValues(scalar)
    } yield {
      // Define predicate function
      (sample: Sample) =>
        sample.getValue(pos).map {
          case d: Datum => cvals.exists(c => scalar.ordering.equiv(c, d))
          case NullData => false
          case d        => throw LatisException(s"Sample value is not a Datum: $d")
        }.getOrElse {
          throw LatisException("Invalid Sample")
        }
    }
  }

}

object Contains {

  def builder: OperationBuilder = (args: List[String]) => fromArgs(args)

  def fromArgs(args: List[String]): Either[LatisException, Contains] = args match {
    case id :: value :: values =>
      Either.fromOption(
        Identifier.fromString(id),
        LatisException(s"Invalid identifier: $id")
      ).map(Contains(_, (value :: values): _*))
    case _ => LatisException("Contains expects a variable identifier and at least one value.").asLeft
  }
}
