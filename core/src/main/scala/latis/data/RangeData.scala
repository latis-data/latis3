package latis.data

/**
 * Convenient construction and extraction methods for the
 * RangeData type alias for Vector[Any].
 */
object RangeData {
  
  /**
   * Construct RangeData from a comma separated list of values.
   */
  def apply(data: Any*): RangeData = data.toVector
  
  /**
   * Construct RangeData from a Seq of values.
   */
  def fromSeq(data: Seq[_]): RangeData = data.toVector
  
  /**
   * Extract a comma separated list of values from RangeData.
   */
  def unapplySeq(d: RangeData): Option[Seq[_]] = Option(d.toSeq)
}
