package latis

package object data {
  //TODO: find better homes for some of these definitions

  /**
   * Define a type alias for DomainData as a List of values of a Data type.
   */
  type DomainData = List[Datum]
  //TODO: require Data with an Eq instance

  /**
   * Define a type alias for RangeData as a List of values of a Data type.
   */
  type RangeData = List[Data]
  //TODO: need to prevent adding a TupleData to a RangeData

  /**
   * Define a SamplePath as a Seq of SamplePositions.
   * Each element in the path implies a nested Function.
   * An empty SamplePath indicates the outer Function itself.
   */
  type SamplePath = List[SamplePosition]
}
