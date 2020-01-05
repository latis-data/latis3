package latis.data

/**
 * Experimental class to enable Dataset.cache to specify which SampledFunction
 * implementation to use for the cache.
 */
abstract class FunctionFactory {

  /**
   * Construct a SampledFunction of the implementing type
   * from a sequence of Samples.
   */
  def fromSamples(samples: Seq[Sample]): MemoizedFunction

  /**
   * Copy the data from the given SampledFunction to
   * a MemoizedFunction of the implementing type.
   * Default to using a potentially unsafe Seq of Samples.
   */
  def restructure(data: SampledFunction): MemoizedFunction =
    fromSamples(data.unsafeForce.sampleSeq)
  //TODO: no-op if same type
}
