package latis.data

/**
 * Represent a SampledFunction of arity zero.
 */
case class ConstantFunction(data: RangeData) extends MemoizedFunction {
  
  def samples: Seq[Sample] = List(Sample(DomainData(), data))

}