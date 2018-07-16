package latis.data

import scala.util._

/**
 * Implement the FunctionData trait with an Iterator of Samples.
 * Evaluation of a StreamingFunction is not allowed since it can
 * guarantee streaming the ordered samples only once. 
 * A Dataset can be memoized with "force" to ensure that it has 
 * FunctionData that can be evaluated.
 */
case class StreamingFunction(samples: Iterator[Sample]) extends FunctionData {
  
  def apply(d: Data): Try[Data] = 
    Failure(new UnsupportedOperationException("Can't evaluate a StreamingFunction."))
}
