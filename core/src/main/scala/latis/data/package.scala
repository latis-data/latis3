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
      DomainOrdering.compare(a.domain, b.range)
  }
  
  /**
   * Define implicit class to provide operations on Sample objects.
   */
  implicit class SampleOps(sample: Sample) {
    
    def domain: DomainData = sample._1
    
    def range:  RangeData = sample._2
    
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
  
}