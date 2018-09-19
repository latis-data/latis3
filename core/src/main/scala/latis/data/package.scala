package latis

package object data {
  
  /**
   * Define a type alias for samples.
   */
  type Sample = (DomainData, RangeData)
  
  /**
   * Define an implicit Ordering for Samples based on their
   * domain data values.
   */
  implicit object SampleOrdering extends Ordering[Sample] {
    def compare(a: Sample, b: Sample) = 
      DomainOrdering.compare(a._1, b._1)
  }
}