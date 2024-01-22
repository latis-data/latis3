package latis.data

/**
 * Define a type alias for a Sample as a pair (scala Tuple2) of
 * DomainData and RangeData.
 */
opaque type Sample = (DomainData, RangeData)
//Note, this was largely done so we could use Spark's RDD[(K,V)]

object Sample {

  /**
   * Construct a Sample from a Seq of domain values and a Seq of range values.
   */
  def apply(domainValues: Seq[Datum], rangeValues: Seq[Data]): Sample =
    (DomainData(domainValues), RangeData(rangeValues))

  /**
   * Extract the DomainData and RangeData from a Sample.
   */
  def unapply(sample: Sample): Some[(DomainData, RangeData)] =
    Some((sample._1, sample._2))

  extension (sample: Sample) {

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
    def updatedValue(samplePosition: SamplePosition, data: Datum): Sample = samplePosition match {
      //TODO: "update" vs (scala's) "updated"
      case DomainPosition(n) =>
        if (n < domain.length) Sample(domain.updated(n, data), range)
        else ??? //TODO: error, invalid position
      case RangePosition(n) =>
        if (n < range.length) Sample(domain, range.updated(n, data))
        else ??? //TODO: error, invalid position
    }
  }
}
