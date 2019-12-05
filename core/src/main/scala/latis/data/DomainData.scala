package latis.data

/**
 * Convenient construction and extraction methods for the
 * DomainData type alias for List[Any].
 */
object DomainData {

  /**
   * Define function to compute the distance between two
   * points defined as DomainData.
   */
  //TODO: distanceFrom method on DomainData? but juat a type alias
  val distance: (DomainData, DomainData) => Double =
    (dd1: DomainData, dd2: DomainData) => {
      //TODO: assert same length
      val squares = dd1.zip(dd2).map {
        case (Number(d1), Number(d2)) => Math.pow((d2 - d1), 2)
        case _ => ??? //TODO: invalid data type
      }
      Math.sqrt(squares.sum)
    }

  /**
   * Construct an empty DomainData.
   */
  def apply(): DomainData = List()

  /**
   * Construct DomainData from a comma separated list of values.
   */
  def apply(d: Data, ds: Data*): DomainData = d +: ds.toList

  /**
   * Construct DomainData from a Seq of values.
   */
  def apply(data: Seq[Data]): DomainData = data.toList

  /**
   * Extract a comma separated list of values from DomainData.
   */
  def unapplySeq(d: DomainData): Option[Seq[Data]] = Option(d)
}
