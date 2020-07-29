package latis.ops

import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.time.Time
import latis.time.TimeScale
import latis.units.UnitConverter
import latis.util.LatisConfig
import latis.util.LatisException

/**
 * Defines an Operation that converts a time tuple to a time scalar
 *
 * @param name the name of the Tuple variable that stores time values
 */
case class TimeTupleToTime(name: String = "time") extends UnaryOperation {
  
  override def applyToModel(model: DataType): DataType = {
    //val timePos: Int = model.getPath(name) match {
    //  case Some(List(RangePosition(n))) => n
    //  case _ => throw new LatisException(s"Cannot find variable: $name")
    //}
    
    val timeTuple = model.findAllVariables(name).head
    
    val time: Scalar = timeTuple match {
      case Tuple(es @ _*) =>
        //build up format string
        val format: String = es.map(e => e("units") match {
          case Some(units) => units
          case None => throw new RuntimeException("A time Tuple must have units defined for each element.")
        }).mkString(" ")

        //make the Time Scalar
        val metadata = Metadata("id" -> "time", "units" -> format, "type" -> "string")
        Scalar(metadata)
    }
    
    //TODO: don't assume Time Tuple is the domain; use model.getPath(name) instead
    model match {
      case Function(_, r) => Function(time, r)
    }
  }

  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
    val samples = data.unsafeForce.sampleSeq //TODO: don't unsafeForce

    val convertedSamples = samples.map {
      case Sample(dd, rd) =>
        //extract text values and join with space
        //TODO: join with delimiter, problem when we use regex?
        //TODO: don't assume Time Tuple is the domain; use model.getPath(name) instead
        val time = dd.map { case d: Datum => d.asString }.mkString(" ")
        Sample(Seq(Data.StringValue(time)), rd)
    }
    
    SampledFunction(convertedSamples)
  }

}
