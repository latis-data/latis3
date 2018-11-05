package latis.data

import fs2.Stream
import cats.effect.IO

/**
 * SampledFunction represent a (potentially lazy) ordered sequence of Samples.
 * Multiple implementations may be available for optimization or ease of use.
 * A SampledFunction may also be evaluated at a given domain value.
 * SampledFunctions are not generally evaluatable unless an interpolation
 * strategy is available. In such cases, Function evaluation may fail
 * with an exception (if an exact match is not found). 
 * Note that StreamingFunctions are limited by being traversable once.
 */
trait SampledFunction {
  
  /**
   * Return a Stream of Samples from this SampledFunction.
   */
  def samples: Stream[IO, Sample]
  
  /**
   * Evaluate this SampledFunction at the given domain value.
   * Return the result as Stream.
   */
  def apply(v: DomainData): Stream[IO, RangeData]
  //TODO: implicit Interpolation strategy
  //TODO: ContinuousFunction if there is an interpolation?
  //TODO: eval with DomainSet, support topologies

}

object SampledFunction {
  
  /**
   * Extract a Stream of Samples from a SampledFunction.
   */
  def unapply(sf: SampledFunction): Option[Stream[IO, Sample]] = Option(sf.samples)
}