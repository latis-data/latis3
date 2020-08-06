package latis.ops

import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.time.Time
import latis.time.TimeScale
import latis.units.UnitConverter
import latis.util.LatisConfig
import latis.util.LatisException

import cats.implicits._

/**
 * Defines an Operation that converts a time tuple to a time scalar
 *
 * @param name the name of the Tuple variable that stores time values
 */
case class TimeTupleToTime(name: String = "time") extends UnaryOperation {
  
  override def applyToModel(model: DataType): DataType = {
    val timeTuple = model.findAllVariables(name).head
    
//    val timeLen: Int = timeTuple match {
//      case t: Tuple => t.elements.length //TODO: is this correct if an element is a nested Tuple? 
//      case _ => throw new LatisException(s"Cannot find variable: $name")
//    }
//    val timePos: Int = model.getPath(name) match {
//      case Some(List(DomainPosition(n))) => n
//      //TODO: what if it's RangePosition instead?
//      case _ => throw new LatisException(s"Cannot find path to variable: $name")
//    }
    
    val time: Scalar = timeTuple match {
      case Tuple(es @ _*) =>
        //build up format string
        val format: String = es.toList.traverse(_("units"))
          .fold(throw new LatisException("A time Tuple must have units defined for each element."))(_.mkString(" "))

        //make the Time Scalar
        val metadata = Metadata("id" -> "time", "units" -> format, "type" -> "string")
        Time(metadata)
    }
    
    //TODO: don't assume Time Tuple is the domain; use model.getPath(name) instead
    //      JK. Recurse through the model, then if it's the time Tuple, replace it with time Scalar.
    //      Maybe define .map on DataType?
    model match {
      case Function(_, r) => Function(time, r)
    }
  }

  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
    val samples = data.unsafeForce.sampleSeq //TODO: don't unsafeForce; extend MapOperation instead of UnaryOperation
    
    val timePos: SamplePosition = model.getPath(name) match {
      case Some(List(sp)) => sp
      case _ => throw new LatisException(s"Cannot find path to variable: $name")
    }
    val timeLen: Int = model.findAllVariables(name).head match {
      case t: Tuple => t.flatten match {
        case tf: Tuple => tf.elements.length //TODO: is this "dimensionality"? Should it be a first class citizen?
      }
      case _ => throw new LatisException(s"Cannot find variable: $name")
    }

    val convertedSamples = samples.map {
      case Sample(dd, rd) =>
        //extract text values and join with space
        //TODO: join with delimiter, problem when we use regex?
        timePos match {
          //TODO: reduce code duplication; all that changes is dd vs rd and replacing Samples domain or range
          case DomainPosition(n) =>
            val time: Datum = {
              val timeData = dd.slice(n, n+timeLen)
              Data.StringValue(
                timeData.map { case d: Datum => d.asString }.mkString(" ")
              )
            }
            val domain = dd.slice(0, n) ++ Seq(time) ++ dd.slice(n+timeLen, dd.length)
            Sample(domain, rd)
          case RangePosition(n)  =>
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
    
    SampledFunction(convertedSamples)
  }

}
