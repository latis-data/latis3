package latis.data

/**
 * SampledFunction represent a (potentially lazy) ordered sequence of Samples.
 * Multiple implementations may be available for optimization or ease of use.
 */
trait SampledFunction {
  def samples: Iterator[Sample]
}

object SampledFunction {
  
  /**
   * Extract an Iterator of Samples from a SampledFunction.
   */
  def unapply(sf: SampledFunction): Option[Iterator[Sample]] = Option(sf.samples)
}