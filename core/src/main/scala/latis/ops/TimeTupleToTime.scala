package latis.ops

import cats.implicits._
import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.time.Time
import latis.util.LatisException

/**
 * Defines an Operation that converts a Tuple of time values to a single time Scalar.
 *
 * @param name the name of the Tuple that stores the time values
 */
case class TimeTupleToTime(name: String = "time") extends MapOperation {

  override def applyToModel(model: DataType): DataType = model.map {
      case t @ Tuple(es @ _*) if t.id == name => //TODO: support aliases with hasName?
        //build up format string
        val format: String = es.toList.traverse(_("units"))
          .fold(throw new LatisException("A time Tuple must have units defined for each element."))(_.mkString(" "))
        //make the time Scalar
        val metadata = t.metadata + ("units" -> format) + ("type" -> "string")
        Time(metadata)
      case v => v
    }

  override def mapFunction(model: DataType): Sample => Sample = {
    val timePos: SamplePosition = model.getPath(name) match {
      case Some(List(sp)) => sp
      case None => throw new LatisException(s"Cannot find path to variable: $name")
      case _ => throw new LatisException(s"Variable '$name' must not be in a nested Function.")
    }
    val timeLen: Int = model.findAllVariables(name) match {
      case Nil               => throw new LatisException(s"Cannot find variable: $name")
      case vs: Seq[DataType] => vs.head match {
        case t: Tuple => t.flatten match {
          case tf: Tuple => tf.elements.length //TODO: is this "dimensionality"? Should it be a first class citizen?
        }
        case _ => throw new LatisException(s"Variable '$name' must be a Tuple.")
      }
    }

    (sample: Sample) => sample match {
      case Sample(dd, rd) =>
        //extract text values and join with space
        //TODO: join with delimiter, problem when we use regex?
        timePos match {
          case DomainPosition(n) =>
            val domain = dd.slice(0, n) ++ Seq(time(n, timeLen, dd)) ++ dd.slice(n+timeLen, dd.length)
            Sample(domain, rd)
          case RangePosition(n) =>
            val range = rd.slice(0, n) ++ Seq(time(n, timeLen, rd)) ++ rd.slice(n+timeLen, rd.length)
            Sample(dd, range)
        }
    }
  }

  /** Helper function to slice a time Datum out of a time Tuple given its position and length. */
  private def time(pos: Int, len: Int, data: Seq[Data]): Datum = {
    val timeData = data.slice(pos, pos+len)
    Data.StringValue(
      timeData.map { case d: Datum => d.asString }.mkString(" ")
    )
  }

}
