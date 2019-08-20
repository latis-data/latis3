package latis

package object data {

  /**
   * Define a type alias for DomainData as a Vector of values of any type.
   */
  type DomainData = Vector[OrderedData]

  /**
   * Define a type alias for RangeData as a Vector of values of any type.
   */
  type RangeData = Vector[Data]

  /**
   * Define a type alias for a Sample as a pair (scala Tuple2) of
   * DomainData and RangeData.
   */
  type Sample = (DomainData, RangeData)

  /**
   * Define a SamplePath as a Seq of SamplePositions.
   * Each element in the path implies a nested Function.
   * An empty SamplePath indicates the outer Function itself.
   */
  type SamplePath = Seq[SamplePosition]

  /**
   * Define an implicit Ordering for Samples based on their
   * domain data values.
   */
  implicit object SampleOrdering extends Ordering[Sample] {
    def compare(a: Sample, b: Sample) =
      DomainOrdering.compare(a.domain, b.domain)
  }

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
      //check index OOB
      case DomainPosition(n) =>
        if (n < domain.length) Some(domain(n))
        else None
      case RangePosition(n) =>
        if (n < range.length) Some(range(n))
        else None
    }

    /**
     * Return a new Sample with the given data value in the given position.
     */
    def updatedValue(samplePosition: SamplePosition, value: Data): Sample = samplePosition match {
      case DomainPosition(n) =>
        if (n < domain.length) value match {
          case data: OrderedData => Sample(domain.updated(n, data), range)
          case _ => ??? //TODO: error, domain data must have ordering
        }
        else ??? //TODO: error, invalid position
      case RangePosition(n) =>
        if (n < range.length) Sample(domain, range.updated(n, value))
        else ??? //TODO: error, invalid position
    }

    //  def zipPositionWithValue: Seq[(SamplePosition, Any)] = {
    //    val d = domain.zipWithIndex.map(p => (DomainPosition(p._2), p._1))
    //    val r = range.zipWithIndex.map(p => (RangePosition(p._2), p._1))
    //    d ++ r
    //  }
  }

  //== Implicit Ordering for Domain Data ====================================//

  /**
   * Define an Ordering for DomainData.
   * DaomainData being compared must have the same number of values (arity).
   * Individual values will be compared pair-wise. If the first pair of
   * values are equal, then the next pair will be compared.
   * This is used by the implicit SampleOrdering defined in the package object.
   */
  implicit object DomainOrdering extends Ordering[DomainData] {

    /**
     * Implement the compare method of the Ordering trait.
     */
    def compare(a: DomainData, b: DomainData) = {
      //TODO: ensure the types are comparable, e.g. number-number
      if (a.length != b.length) {
        val msg = "Can't compare DomainData with different arity."
        throw new UnsupportedOperationException(msg)
      } else {
        val pairs: Seq[(OrderedData, OrderedData)] = a zip b
        comparePairs(pairs)
      }
    }

    /**
     * Helper method to compare two sequences of DomainData recursively.
     * If the first pair matches, recursively test the next pair.
     */
    private def comparePairs(ps: Seq[(OrderedData, OrderedData)]): Int =
      ps.toList match {
        case Nil => 0 //all pairs matched
        case head :: tail => 
          val comp = (head._1, head._2) match {
            case (a: Number, b: Number) => a compare b
            case (a: Text, b: Text)     => a compare b
            //TODO:  Compare number to text?
            //case (a: NumberData, b: TextData) => a compare b.stringValue.toDouble
            case _ => ??? //TODO: error, invalid pair, check above, use PartiallyOrdered?
          }
          comp match {
            case 0 => comparePairs(tail) //recurse
            case c => c
          }
      }

  }
}