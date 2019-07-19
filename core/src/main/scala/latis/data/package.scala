package latis

package object data {

  /**
   * Define a type alias for DomainData as a Vector of values of any type.
   */
  type DomainData = Vector[Any]

  /**
   * Define a type alias for RangeData as a Vector of values of any type.
   */
  type RangeData = Vector[Any]

  /**
   * Define a type alias for a Sample as a pair (scala Tuple2) of
   * DomainData and RangeData.
   */
  type Sample = (DomainData, RangeData)

  /**
   * Define a SamplePath as a Seq of SamplePositions.
   * Each element in the path implies a nested Function.
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
    def getValue(samplePosition: SamplePosition): Option[Any] = samplePosition match {
      //check index OOB
      case DomainPosition(n) =>
        if (n < domain.length) Some(domain(n))
        else None
      case RangePosition(n) =>
        if (n < range.length) Some(range(n))
        else None
    }

    /**
     * Return a new Sample with the given value in the given position.
     */
    def updatedValue(samplePosition: SamplePosition, value: Any): Sample = samplePosition match {
      //TODO: insert values if Seq[Any]
      case DomainPosition(n) =>
        if (n < domain.length) Sample(domain.updated(n, value), range)
        else ??? //TODO: error
      case RangePosition(n) =>
        if (n < range.length) Sample(domain, range.updated(n, value))
        else ??? //TODO: error
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
   * This is used by the implicit SampleOrdering
   * defined in the package object.
   */
  implicit object DomainOrdering extends Ordering[DomainData] {
    /*
   * TODO: what about DomainData extends Ordered?
   * impl one, get the other implicitly?
   * But just an alias for Vector right now
   * could save us from explicitly using this
   */

    /**
     * Implement the compare method of the Ordering trait.
     */
    def compare(a: DomainData, b: DomainData) = {
      if (a.length != b.length) {
        val msg = "Can't compare DomainData with different arity."
        throw new UnsupportedOperationException(msg)
      } else comparePairs(a zip b)(ScalarOrdering)
    }

    /**
     * Helper method to compare two sequences of DomainData recursively.
     * If the first pair matches, recursively test the next pair.
     */
    private def comparePairs[T](ps: Seq[(T, T)])(implicit ord: Ordering[T]): Int =
      ps.toList match {
        case Nil => 0 //all pairs matched
        case head :: tail => ord.compare(head._1, head._2) match {
          case 0      => comparePairs(tail) //recurse
          case c: Int => c
        }
      }

  }
}