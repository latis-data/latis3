package latis.ops

import latis.data._
import latis.metadata._
import latis.model._

/**
 * A Filter is a unary Operation that applies a boolean
 * predicate to each Sample of the given Dataset resulting
 * in a new Dataset that has all the "false" Samples removed.
 * This only impacts the number of Samples in the Dataset. 
 * It does not affect the model.
 */
trait Filter extends UnaryOperation {
  //TODO: update "length" metadata?
  //TODO: clarify behavior of nested Functions: all or none
  
  /**
   * Create a function that specifies whether a Sample
   * should be kept.
   */
  def makePredicate(model: DataType): Sample => Boolean
  
  /**
   * Delegate to the Dataset's SampledFunction to apply the predicate
   * and generate a new SampledFunction
   */
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction =
    data.filter(makePredicate(model))

}