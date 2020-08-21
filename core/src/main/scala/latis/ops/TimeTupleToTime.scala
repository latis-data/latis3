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

  override def applyToModel(model: DataType): DataType = {
    val timeTuple = model.findAllVariables(name) match {
      case Nil               => throw new LatisException(s"Cannot find variable: $name")
      case vs: Seq[DataType] => vs.head
    }

    val time: Scalar = timeTuple match {
      case Tuple(es @ _*) =>
        //build up format string
        val format: String = es.toList.traverse(_("units"))
          .fold(throw new LatisException("A time Tuple must have units defined for each element."))(_.mkString(" "))
        //make the time Scalar
        val metadata = Metadata("id" -> "time", "units" -> format, "type" -> "string")
        Time(metadata)
      case _ => throw new LatisException(s"Variable '$name' must be a Tuple.")
    }
    
    model.map {
      case t: Tuple if t.id == name => time //TODO: support aliases with hasName?
      case v => v
    }
  }

  override def mapFunction(model: DataType): Sample => Sample = {
    val timePos: SamplePosition = model.getPath(name) match {
      case Some(List(sp)) => sp
      case _ => throw new LatisException(s"Cannot find path to variable: $name")
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
          //TODO: reduce code duplication? Only difference is dd vs rd (types don't match) and replacing domain vs range
          case DomainPosition(n) =>
            val time: Datum = {
              val timeData = dd.slice(n, n+timeLen)
              Data.StringValue(
                timeData.map { case d: Datum => d.asString }.mkString(" ")
              )
            }
            val domain = dd.slice(0, n) ++ Seq(time) ++ dd.slice(n+timeLen, dd.length)
            Sample(domain, rd)
          case RangePosition(n) =>
            val time: Datum = {
              val timeData = rd.slice(n, n+timeLen)
              Data.StringValue(
                timeData.map { case d: Datum => d.asString }.mkString(" ")
              )
            }
            val range = rd.slice(0, n) ++ Seq(time) ++ rd.slice(n+timeLen, dd.length)
            Sample(dd, range)
        }
    }
  }

}
