package latis.data

import scala.util.Try


/**
 * EvaluatableFunction represents Function Data that can be evaluated.
 * SampledFunctions aren't generally evaluatable unless an interpolation
 * strategy is available. In such cases, Function evaluation may fail
 * with an exception (if an exact match is not found).
 * Note that StreamingFunctions which only promise an Iterator of Samples 
 * do not support this interface due to TraversableOnce limitations.
 */
trait EvaluatableFunction extends SampledFunction {
  def apply(v: Data): Try[Data]
}
