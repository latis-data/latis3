package latis.data

/**
 * Convenient construction and extraction methods for the
 * RangeData type alias for Vector[Any].
 */
object RangeData {
  
  /**
   * Construct RangeData from a comma separated list of values.
   */
  def apply(data: Data*): RangeData = data.toVector
  
  /**
   * Construct RangeData from a Seq of values.
   */
  def fromSeq(data: Seq[Data]): RangeData = data.toVector
  
  /**
   * Extract a comma separated list of values from RangeData.
   */
  def unapplySeq(d: RangeData): Option[Seq[Data]] = Option(d.toSeq)
}
