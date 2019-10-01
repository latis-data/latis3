package latis.ops

import latis.data._
import latis.metadata._
import latis.model._


/**
 * An Operation that maps a function of RangeData => RangeData
 * over the data of a Dataset to generate a new Dataset
 * with only the range of each Sample modified. The resulting 
 * Dataset should have the same number of Samples.
 */
trait MapRangeOperation extends UnaryOperation {
  
  /**
   * Define a function that modifies RangeData.
   */
  def mapFunction(model: DataType): RangeData => RangeData
  
  /**
   * Delegate to the Dataset's SampledFunction to apply the "map" function
   * and generate a new SampledFunction
   */
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction =
    data.mapRange(mapFunction(model))
  
}
