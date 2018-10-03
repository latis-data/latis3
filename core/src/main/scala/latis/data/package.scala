package latis

package object data {
  
  /**
   * Define a type alias for DomainData as an array of values of any type.
   */
  type DomainData = Array[Any]
  
  /**
   * Define a type alias for RangeData as an array of values of any type.
   */
  type RangeData = Array[Any]
  
  /**
   * Define a type alias for samples as a pair of
   * DomainData and RangeData.
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