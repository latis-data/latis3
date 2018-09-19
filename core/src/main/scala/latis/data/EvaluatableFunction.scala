package latis.data

import scala.util.Try


/**
 * EvaluatableFunction represents a SampledFunction that can be evaluated.
 * SampledFunctions aren't generally evaluatable unless an interpolation
 * strategy is available. In such cases, Function evaluation may fail
 * with an exception (if an exact match is not found). Thus the result
 * of evaluation is a Success or Failure which can encapsulate the exception.
 * Note that StreamingFunctions, which only promise an Iterator of Samples, 
 * do not support this interface due to TraversableOnce limitations.
 */
trait EvaluatableFunction extends SampledFunction {
  def apply(v: DomainData): Try[RangeData]
}
