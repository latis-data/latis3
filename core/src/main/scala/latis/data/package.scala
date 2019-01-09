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
}