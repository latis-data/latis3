package latis.ops

import cats.syntax.all._

import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.time.Time
import latis.util.Identifier
import latis.util.Identifier.IdentifierStringContext
import latis.util.LatisException

/**
 * Defines an Operation that converts a Tuple of time values to a single time Scalar.
 *
 * @param id the identifier of the Tuple that stores the time values
 * TODO: using id"time" here yields macro implementation not found error. Why?
 */
case class TimeTupleToTime(id: Identifier = id"time") extends MapOperation {

  def applyToModel(model: DataType): Either[LatisException, DataType] =
    Either.catchOnly[LatisException](model.map {
    case t @ Tuple(es @ _*) if t.id.contains(id) => //TODO: support aliases with hasName?
      //build up format string
      val format: String = es.collect{case s: Scalar => s}.toList.traverse(_.units)
        .fold(throw LatisException("A time Tuple must have units defined for each element."))(_.mkString(" "))
      //make the time Scalar
      //assumes time tuple has id
      val metadata = Metadata(
        "id"    -> t.id.get.asString,
        "units" -> format,
        "type"  -> "string"
      )
      Time.fromMetadata(metadata).fold(throw _, identity)
    case v => v
    })

  override def mapFunction(model: DataType): Sample => Sample = {
    val timePos: SamplePosition = model.findPath(id) match {
      case Some(List(sp)) => sp
      case None => throw LatisException(s"Cannot find path to variable: ${id.asString}")
      case _ => throw LatisException(s"Variable '${id.asString}' must not be in a nested Function.")
    }
    val timeLen: Int = model.findVariable(id)
      .getOrElse {
        throw new LatisException(s"Cannot find variable: ${id.asString}")
      } match {
          case t: Tuple => t match { //TODO: was t.flatten
            case tf: Tuple => tf.elements.length //TODO: is this "dimensionality"? Should it be a first class citizen?
            case _ => throw LatisException("Tuple did not flatten to a tuple.")
          }
          case _ => throw LatisException(s"Variable '${id.asString}' must be a Tuple.")
      }

    (sample: Sample) =>
      //extract text values and join with space
      //TODO: join with delimiter, problem when we use regex?
      timePos match {
        case DomainPosition(n) =>
          val domain = DomainData.fromData(convertTimeTuple(sample.domain, n, timeLen)) match {
            case Right(d) => d
            case Left(ex) => throw ex
          }
          Sample(domain, sample.range)
        case RangePosition(n) =>
          val range = convertTimeTuple(sample.range, n, timeLen)
          Sample(sample.domain, range)
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
      case id :: Nil => TimeTupleToTime(
        Identifier.fromString(id).getOrElse {
          throw LatisException(s"'$id' is not a valid identifier")
        }
      )
      case _ => throw LatisException("Too many arguments to TimeTupleToTime")
    }
  }
}
