package latis.data

/**
 * Convenient construction and extraction methods for the
 * DomainData type alias for Vector[Any].
 */
object DomainData {
  
  /**
   * Construct DomainData from a comma separated list of values.
   */
  def apply(data: Any*): DomainData = data.toVector
  
  /**
   * Construct DomainData from a Seq of values.
   */
  def fromSeq(data: Seq[_]): DomainData = data.toVector
  
  /**
   * Extract a comma separated list of values from DomainData.
   */
  def unapplySeq(d: DomainData): Option[Seq[_]] = Option(d.toSeq)
}
