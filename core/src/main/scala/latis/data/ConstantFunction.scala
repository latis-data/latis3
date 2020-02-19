package latis.data

/**
 * Represent a SampledFunction of arity zero.
 */
case class ConstantFunction(data: Data) extends MemoizedFunction {

  def sampleSeq: Seq[Sample] = List(Sample(DomainData(), RangeData(data)))

}
