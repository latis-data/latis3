package latis.data

/**
 * Define a single Sample of a SampledFunction.
 * It consists of data values separated by
 * domain and range.
 */
case class Sample(domain: DomainData, range: RangeData) {

  /**
   * Get the data values from this Sample at the given SamplePosition.
   * The value could represent a Scalar variable or a nested Function.
   */
  def getValue(samplePosition: SamplePosition): Option[Any] = samplePosition match {
    //check index OOB
    case DomainPosition(n) =>
      if (n < domain.length) Some(domain(n))
      else None
    case RangePosition(n) =>
      if (n < range.length) Some(range(n))
      else None
  }
  
  /**
   * Return a new Sample with the given value in the given position.
   */
  def updateValue(samplePosition: SamplePosition, value: Any): Sample = samplePosition match {
    //TODO: insert values if Seq[Any]
    case DomainPosition(n) =>
      if (n < domain.length) Sample(domain.updated(n, value), range)
      else ??? //TODO: error
    case RangePosition(n) =>
      if (n < range.length) Sample(domain, range.updated(n, value))
      else ??? //TODO: error
  }
  
//  def zipPositionWithValue: Seq[(SamplePosition, Any)] = {
//    val d = domain.zipWithIndex.map(p => (DomainPosition(p._2), p._1))
//    val r = range.zipWithIndex.map(p => (RangePosition(p._2), p._1))
//    d ++ r
//  }
}


object Sample {

  /**
   * Construct a Sample from a Seq of domain values and a Seq of range values.
   */
  def apply(domainValues: Seq[Any], rangeValues: Seq[Any]): Sample = 
    Sample(DomainData.fromSeq(domainValues), RangeData.fromSeq(rangeValues))

}
