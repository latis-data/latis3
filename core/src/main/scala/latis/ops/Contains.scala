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

  def predicate(model: DataType): Sample => Boolean = {
    // Determine the path to the selected variable
    val path: SamplePosition = model.findPath(id) match {
      case Some(p) => p.head //assume no nested functions for now
      case None => throw LatisException(s"Variable not found: ${id.asString}")
    }

    // Get the target Scalar variable
    val scalar: Scalar = model.findVariable(id) match {
      case Some(s: Scalar) => s
      case _ => throw LatisException(s"Scalar variable not found: ${id.asString}")
    }

    val ordering = scalar.ordering

    // Convert values to appropriate type for comparison
    val cvals: Seq[Datum] = values.map { v =>
      scalar.convertValue(v).getOrElse {
        val msg = s"Invalid comparison value: $v"
        throw LatisException(msg)
      }
    }

    // Define predicate function
    (sample: Sample) =>
      sample.getValue(path).map {
        case d: Datum => cvals.exists(c => ordering.equiv(c, d))
        case NullData => false
        case d        => throw LatisException(s"Sample value is not a Datum: $d")
      }.getOrElse {
        throw LatisException("Invalid Sample")
      }
  }

}

object Contains {

  def fromArgs(args: List[String]): Either[LatisException, Contains] = args match {
    //TODO: support quoted string literals
    case id :: value :: values =>
      Either.fromOption(
        Identifier.fromString(id),
        LatisException(s"Invalid identifier: $id")
      ).map(Contains(_, (value :: values): _*))
    case _ => LatisException("Contains expects a variable identifier and at least one value.").asLeft
  }
}
