package latis.data

object Sample {

  /**
   * Construct a Sample from a Seq of domain values and a Seq of range values.
   */
  def apply(domainValues: Seq[Any], rangeValues: Seq[Any]): Sample =
    (DomainData.fromSeq(domainValues), RangeData.fromSeq(rangeValues))

  /**
   * Extract the DomainData and RangeData from a Sample.
   */
  def unapply(sample: Sample): Option[(DomainData, RangeData)] = 
    Some(sample._1, sample._2)
}
