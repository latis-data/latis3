package latis

package object data {
  //TODO: find better homes for some of these definitions

  /**
   * Define a type alias for DomainData as a List of values of a Data type.
   */
  type DomainData = List[Data]
  //TODO: require Data with an Eq instance

  /**
   * Define a type alias for RangeData as a List of values of a Data type.
   */
  type RangeData = List[Data]

  /**
   * Define a type alias for a Sample as a pair (scala Tuple2) of
   * DomainData and RangeData.
   */
  type Sample = (DomainData, RangeData)
  //Note, this was largely done so we could use Spark's RDD[(K,V)]

  /**
   * Define a SamplePath as a Seq of SamplePositions.
   * Each element in the path implies a nested Function.
   * An empty SamplePath indicates the outer Function itself.
   */
  type SamplePath = List[SamplePosition]

  /**
   * Define implicit class to provide operations on Sample objects.
   */
  implicit class SampleOps(sample: Sample) {

    def domain: DomainData = sample._1

    def range: RangeData = sample._2

    /**
     * Get the data values from this Sample at the given SamplePosition.
     * The value could represent a Scalar variable or a nested Function.
     */
    def getValue(samplePosition: SamplePosition): Option[Data] = samplePosition match {
      case DomainPosition(n) => domain.lift(n)
      case RangePosition(n)  => range.lift(n)
    }

    /**
     * Return a new Sample with the given data value in the given position.
     */
    def updatedValue(samplePosition: SamplePosition, data: Data): Sample = samplePosition match {
      //TODO: "update" vs (scala's) "updated"
      case DomainPosition(n) =>
        if (n < domain.length) Sample(domain.updated(n, data), range)
        else ??? //TODO: error, invalid position
      case RangePosition(n) =>
        if (n < range.length) Sample(domain, range.updated(n, data))
        else ??? //TODO: error, invalid position
    }

    //  def zipPositionWithValue: Seq[(SamplePosition, Any)] = {
    //    val d = domain.zipWithIndex.map(p => (DomainPosition(p._2), p._1))
    //    val r = range.zipWithIndex.map(p => (RangePosition(p._2), p._1))
    //    d ++ r
    //  }
  }

  /**
   * Placeholder until we get ordering properly enabled.
   */
  implicit object DomainOrdering extends Ordering[DomainData] {
    def compare(d1: DomainData, d2: DomainData): Int = ???
  }

  implicit object SampleOrdering extends Ordering[Sample] {
    def compare(s1: Sample, s2: Sample): Int =
      DomainOrdering.compare(s1.domain, s2.domain)
  }

  implicit object NumberOrdering extends Ordering[Number] {
    def compare(n1: Number, n2: Number): Int =
      n1.asDouble.compare(n2.asDouble)
  }

  implicit object TextOrdering extends Ordering[Text] {
    def compare(t1: Text, t2: Text): Int =
      t1.asString.compare(t2.asString)
  }
}
