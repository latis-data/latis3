package latis.data

/**
 * Represent a SampledFunction of arity zero.
 */
case class ConstantFunction(data: RangeData) extends MemoizedFunction {

  /*
  TODO: eval a CF
    returns the nested MF as RangeData
    could then get our hands on the MF
    does eval in general requre unsafe run?
   */

  def sampleSeq: Seq[Sample] = List(Sample(DomainData(), data))

}

object ConstantFunction {

  def apply(data: Data): ConstantFunction =
    ConstantFunction(List(data))
}
