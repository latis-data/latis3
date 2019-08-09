package latis.ops

import latis.data.Sample
import latis.model.DataType

/**
 * Compose two MapOperations by composing their map functions.
 */
case class ComposedOperation(mop1: MapOperation, mop2: MapOperation) extends MapOperation {
  
  def makeMapFunction(model: DataType): Sample => Sample = {
    val f1 = mop1.makeMapFunction(model)
    val f2 = mop2.makeMapFunction(mop1.applyToModel(model))
    f1 andThen f2
  }
    
  override def applyToModel(model: DataType): DataType = 
    mop2.applyToModel(mop1.applyToModel(model))
    
  //TODO: update metadata
}