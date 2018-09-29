package latis.data

/**
 * Convenience construction and extraction methods for Samples.
 * Sample is defined to be the Tuple2 (DomainData, RangeData)
 * in the package object.
 * Note that Sample takes (and extracts) collections (Seq, Array),
 * while DomainData and RangeData use values (varargs).
 */
object Sample {
  
  /**
   * Construct a Sample from a Seq of domain values and a Seq of range values.
   */
  def apply(ds: Seq[Any], rs: Seq[Any]): Sample = 
    (DomainData.fromSeq(ds), RangeData.fromSeq(rs))
  
    /**
     * Extract Sample values as a pair of Arrays for the domain and range values.
     * Note that this is the definition of the Sample type alias but this allows
     * us to pattern match with "Sample".
     */
  def unapply(sample: Sample): Option[(Array[Any], Array[Any])] = sample match {
    case (ds, rs) => Option((ds, rs))
  }

}
