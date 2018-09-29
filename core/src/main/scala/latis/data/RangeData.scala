package latis.data

/**
 * Convenient construction and extraction methods for the
 * RangeData type alias for Array[Any].
 */
object RangeData {
  
  /**
   * Construct RangeData from a comma separated list of values.
   */
  def apply(data: Any*): RangeData = data.toArray
  
  /**
   * Construct RangeData from a Seq of values.
   */
  def fromSeq(data: Seq[_]): RangeData = data.toArray
  
  /**
   * Extract a comma separated list of values from RangeData.
   */
  def unapplySeq(d: RangeData): Option[Seq[_]] = Option(d.toSeq)
}
