package latis.data

/**
 * Implement a SampledFunction with a Seq of Samples.
 */
case class SeqFunction(sampleSeq: Seq[Sample]) extends MemoizedFunction

/*
 * TODO: use Iterable and change to IterableFunction?
 * but don't want IterableOnce
 * probably want to limit it to IndexedSeq
 */

object SeqFunction {

  def apply(sample: Sample): SeqFunction = SeqFunction(Seq(sample))
}
