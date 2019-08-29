package latis.data

/**
 * Convenient construction and extraction methods for the
 * RangeData type alias for List[Any].
 */
object RangeData {
  
  /**
   * Construct RangeData from a comma separated list of values.
   */
  def apply(d: Data, ds: Data*): RangeData = d +: ds.toList
  
  /**
   * Construct RangeData from a Seq of values.
   */
  def apply(data: Seq[Data]): RangeData = data.toList
  
  /**
   * Extract a comma separated list of values from RangeData.
   */
  def unapplySeq(d: RangeData): Option[Seq[Data]] = Option(d.toSeq)
}
