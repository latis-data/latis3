package latis.data


/**
 * Implement a SampledFunction with a Seq of Samples.
 */
case class SeqFunction(samples: Seq[Sample]) extends MemoizedFunction 

//TODO: use Iterable and change to IterableFunction?