package latis.data

/**
 * Represents a SampledFunction of arity zero.
 */
case class ConstantFunction(data: Data) extends MemoizedFunction {

  //TODO: allow ord to be passed in, may want to compare?
  def ordering: Option[PartialOrdering[DomainData]] = None

  def sampleSeq: Seq[Sample] = List(Sample(DomainData(), RangeData(data)))

}
