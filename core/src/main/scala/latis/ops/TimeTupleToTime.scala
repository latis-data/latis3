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
    val timeTuple = model.findAllVariables(name)(0)
    
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
    
    model //TODO: replace time Tuple with time Scalar in model
  }

  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
    data
  }

}
