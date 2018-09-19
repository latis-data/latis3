package latis.data

/**
 * Implement the SampledFunction trait with an Iterator of Samples.
 * Note that evaluation of a StreamingFunction is not allowed since it can
 * guarantee streaming the ordered samples only once. 
 * A Dataset can be memoized with "force" to ensure that it has a
 * SampledFunction that can be evaluated.
 */
case class StreamingFunction(samples: Iterator[Sample]) extends SampledFunction
