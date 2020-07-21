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
    val samples = data.unsafeForce.sampleSeq
//    val timePos: Int = model.getPath(name) match {
//      case Some(List(RangePosition(n))) => n
//      case _ => throw new LatisException(s"Cannot find variable: $name")
//    }
    
    //samples.map
    
    data
  }

}
