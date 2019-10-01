package latis.ops

import latis.data._
import latis.metadata._
import latis.model._

/**
 * An Operation that maps a function of Sample => Sample
 * over the data of a Dataset to generate a new Dataset
 * with each Sample modified. The resulting Dataset should
 * have the same number of Samples.
 */
trait MappingOperation extends UnaryOperation with StreamingOperation { self =>
  
  /**
   * Define a function that modifies a given Sample
   * into a new Sample.
   */
  def mapFunction(model: DataType): Sample => Sample
  
  /**
   * Compose with a MappingOperation.
   * Note that the MappingOperation will be applied first.
   * This satisfies the StreamingOperation trait.
   */
  def compose(mappingOp: MappingOperation) = new MappingOperation {
    //TODO: apply to metadata
    
    def mapFunction(model: DataType): Sample => Sample = 
      mappingOp.mapFunction(model) andThen 
      self.mapFunction(mappingOp.applyToModel(model))
    
    override def applyToModel(model: DataType): DataType = 
      self.applyToModel(mappingOp.applyToModel(model))
  }
  
  /**
   * Delegate to the Dataset's SampledFunction to apply the "map" function
   * and generate a new SampledFunction
   */
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction =
    data.map(mapFunction(model))
  
}
