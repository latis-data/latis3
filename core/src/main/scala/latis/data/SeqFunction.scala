package latis.data

/**
 * Implements a SampledFunction with a Seq of Samples.
 * Note that the default ordering assumes a Cartesian
 * domain set which we can't guarantee here.
 */
case class SeqFunction(
  sampleSeq: Seq[Sample],
  ordering: Option[PartialOrdering[DomainData]] = None
) extends MemoizedFunction

object SeqFunction extends FunctionFactory {

  def apply(sample: Sample): SeqFunction = SeqFunction(Seq(sample))

  def fromSamples(samples: Seq[Sample]): SeqFunction = SeqFunction(samples)
}
