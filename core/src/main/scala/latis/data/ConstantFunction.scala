package latis.data

/**
 * Represent a SampledFunction of arity zero.
 */
case class ConstantFunction(data: RangeData) extends MemoizedFunction {
  
  def samples: Seq[Sample] = List(Sample(DomainData(), data))
  
  /*
   * can we write as 0 -> a?
   *   might be helpful with algebraic proofs
   * should Scalar and Tuple extend CF so we can always ask for domain and range?
   */
}