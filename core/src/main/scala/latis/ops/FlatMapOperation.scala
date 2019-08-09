package latis.ops

import latis.data._
import latis.model._

/**
 * A FlatMapOperation provides a function that turns a Sample
 * into a MemoizedFunction.
 * ?nested Function with orig domain?
 *   not in general
 *   groupBy creates a new domain
 *   can we use flatMap generally?
 *   basically replacing one Sample with a Seq of Samples
 *  *GroupBy can't simply do one sample at a time, must shuffle data
 *     maybe withAgg only makes sense there?
 *   Uncurry can use this, but not useful to add agg?
 *   Curry is like GroupBy but preserves order so no shuffle needed if we are careful with our partitioner
 */
trait FlatMapOperation extends UnaryOperation { //outer =>
  
  def flatMapFunction: Sample => MemoizedFunction
  
  override def applyToData(data: SampledFunction, model: DataType) = data flatMap { s =>
    flatMapFunction(s)
  }
    
//  /**
//   * Apply an Aggregation Operation to the results of this FlatMapOperation
//   * to produce a MapOperation.
//   */
//  def withAggregation(agg: Aggregation): MapOperation = new MapOperation {
//    def makeMapFunction(model: DataType): Sample => Sample = (sample: Sample) => {
//      agg.applyToData(flatMapFunction(sample), outer.applyToModel(model)) match {
//        case ConstantFunction(range) => Sample(sample.domain, range)
//      }
//    }
//    
//    def applyToModel(model: DataType): DataType = {
//      outer.applyToModel(model) match {
//        case Function(domain, range) => 
//      }
//    }
//    //TODO: apply to model and metadata, twice since we are combining two operations
//  }
}