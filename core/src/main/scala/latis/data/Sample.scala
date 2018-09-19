package latis.data

/**
 * Convenience construction and extraction methods for Samples.
 * Sample is defined to be the Tuple2 (DomainData, RangeData)
 * in the package object.
 * Note that Sample takes (and extracts) sequences (Seq),
 * while DomainData and RangeData use varargs.
 */
object Sample {
  
  /**
   * Construct a Sample from a Seq of domain values and a Seq of range values.
   */
  def apply(ds: Seq[Any], rs: Seq[Any]): Sample = 
    (DomainData.fromSeq(ds), RangeData.fromSeq(rs))
  
    /**
     * Extract Sample values as a pair of Seq for the domain and range values.
     */
  def unapply(sample: Sample): Option[(Seq[_], Seq[_])] = sample match {
    case (DomainData(ds @ _*), RangeData(rs @ _*)) => Option((ds, rs))
  }
}
