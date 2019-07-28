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
  //TODO: distanceFrom method on DomainData? but juat a type alias
  val distance = (dd1: DomainData, dd2: DomainData) => {
    //TODO: assert same length
    val squares = (dd1 zip dd2) map {
      case (Number(d1), Number(d2)) => Math.pow((d2 - d1), 2)
      case _ => ??? //TODO: invalid data type
    }
    Math.sqrt(squares.sum)
  }
  
  /**
   * Construct DomainData from a comma separated list of values.
   */
  def apply(data: OrderedData*): DomainData = data.toVector
  
  /**
   * Construct DomainData from a Seq of values.
   */
  def fromSeq(data: Seq[OrderedData]): DomainData = data.toVector
  
  /**
   * Extract a comma separated list of values from DomainData.
   */
  def unapplySeq(d: DomainData): Option[Seq[OrderedData]] = Option(d.toSeq)
}
