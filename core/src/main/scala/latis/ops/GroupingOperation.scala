package latis.ops

import latis.data._
import latis.model._

trait GroupingOperation extends UnaryOperation {
  
  /**
   * Specify an Aggregation Operation to use to reduce
   * a collection of Samples to a single RangeData.
   */
  def aggregation: Aggregation
  
  /**
   * Define a function that optionally creates a DomainData
   * from a Sample. These DomainData be used to define the 
   * domain set of the resulting SampledFunction.
   */
  def groupByFunction: Sample => Option[DomainData]
  
  /**
   * Override to construct the new SampledFunction by
   * delegating to the original SampledFunction's
   * groupBy method.
   */
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction =
    data.groupBy(groupByFunction, aggregation)
    
}

