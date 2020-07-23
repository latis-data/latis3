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
    model
  }

  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
//    val samples = data.unsafeForce.sampleSeq
//    val timePos: Int = model.getPath/*WithoutFlattening*/(name) match {
//      case Some(List(RangePosition(n))) => n
//      case _ => throw new LatisException(s"Cannot find variable: $name")
//    }
    
    val x = model.flatten
    
    val timeTuple = x.findAllVariables("time")
    
    timeTuple(0) match {
      case Tuple(es @ _*) =>
        //extract text values and join with space
        //TODO: join with delimiter, problem when we use regex
        val namez = es.map(e => e match { case _: Scalar => e.id}).toSeq.mkString(" ")
        //build up format string
        val format = es.map(e => e("units") match {
          case Some(units) => units
          case None => throw new RuntimeException("A time Tuple must have units defined for each element.")
        }).mkString(" ")
        
        //make the Time Scalar
        val metadata = Metadata("id" -> "time", "units" -> format, "type" -> "string")
        val time = Scalar(metadata)
        Some(time)
    }
    
    data
  }

}
