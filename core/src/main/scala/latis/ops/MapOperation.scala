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
trait MapOperation extends UnaryOperation {
  
  /**
   * Define a function that modifies a given Sample
   * into a new Sample.
   */
  def makeMapFunction(model: DataType): Sample => Sample
  
  /**
   * Delegate to the Dataset's SampledFunction to apply the "map" function
   * and generate a new SampledFunction
   */
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction =
    data.map(makeMapFunction(model))
  
}