package latis.data

/**
 * Convenient construction and extraction methods for the
 * DomainData type alias for Vector[Any].
 */
object DomainData {

  /**
   * Define function to compute the distance between two 
   * points defined as DomainData.
   */
  //TODO: distanceFrom method on DomainData?
  val distance = (dd1: DomainData, dd2: DomainData) => {
    //TODO: assert same length
    //TODO: support any Numeric
    val squares = (dd1 zip dd2) map {
      case (d1: Double, d2: Double) => Math.pow((d2 - d1), 2)
      case _                        => ??? //TODO: invalid data type
    }
    Math.sqrt(squares.sum)
  }
  
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
