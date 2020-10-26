package latis.ops

import cats.syntax.all._

import latis.data._
import latis.model._
import latis.time.Time
import latis.util.Identifier
import latis.util.Identifier.IdentifierStringContext
import latis.util.LatisException

/**
 * Defines an Operation that converts a Tuple of time values to a single time Scalar.
 *
 * @param name the identifier of the Tuple that stores the time values
 * TODO: using id"time" here yields macro implementation not found error. Why?
 */
case class TimeTupleToTime(name: Identifier = id"time") extends MapOperation {

  def applyToModel(model: DataType): Either[LatisException, DataType] =
    Either.catchOnly[LatisException](model.map {
    case t @ Tuple(es @ _*) if t.id.contains(name) => //TODO: support aliases with hasName?
      //build up format string
      val format: String = es.toList.traverse(_("units"))
        .fold(throw LatisException("A time Tuple must have units defined for each element."))(_.mkString(" "))
      //make the time Scalar
      val metadata = t.metadata + ("units" -> format) + ("type" -> "string")
      Time(metadata)
    case v => v
    })

  override def mapFunction(model: DataType): Sample => Sample = {
    val timePos: SamplePosition = model.getPath(name) match {
      case Some(List(sp)) => sp
      case None => throw LatisException(s"Cannot find path to variable: ${name.asString}")
      case _ => throw LatisException(s"Variable '${name.asString}' must not be in a nested Function.")
    }
    val timeLen: Int = model.findVariable(name)
      .getOrElse {
        throw new LatisException(s"Cannot find variable: ${name.asString}")
      } match {
          case t: Tuple => t.flatten match {
            case tf: Tuple => tf.elements.length //TODO: is this "dimensionality"? Should it be a first class citizen?
            case _ => throw LatisException("Tuple did not flatten to a tuple.")
          }
          case _ => throw LatisException(s"Variable '${name.asString}' must be a Tuple.")
        }

    { case Sample(dd, rd) =>
      //extract text values and join with space
      //TODO: join with delimiter, problem when we use regex?
      timePos match {
        case DomainPosition(n) =>
          val domain = DomainData.fromData(convertTimeTuple(dd, n, timeLen)) match {
            case Right(d) => d
            case Left(ex) => throw ex
          }
          Sample(domain, rd)
        case RangePosition(n) =>
          val range = convertTimeTuple(rd, n, timeLen)
          Sample(dd, range)
      }
    }
  }

  /**
   * Helper function to slice a time Scalar out of a time Tuple
   * given its position within a list of Data and its length.
   */
  private def convertTimeTuple(data: Seq[Data], pos: Int, len: Int): Seq[Data] = {
    val timeDatum = {
      val timeData = data.slice(pos, pos+len)
      Data.StringValue(
        timeData.map {
          case d: Datum => d.asString
          case _ => throw LatisException("Time tuple is not a tuple")
        }.mkString(" ")
      )
    }
    data.slice(0, pos) ++ Seq(timeDatum) ++ data.slice(pos+len, data.length)
  }

}

object TimeTupleToTime {

  def fromArgs(args: List[String]): Either[LatisException, TimeTupleToTime] = Either.catchOnly[LatisException] {
    args match {
      case Nil => TimeTupleToTime()
      case name :: Nil => TimeTupleToTime(
        Identifier.fromString(name).getOrElse {
          throw LatisException(s"'$name' is not a valid identifier")
        }
      )
      case _ => throw LatisException("Too many arguments to TimeTupleToTime")
    }
  }
}
