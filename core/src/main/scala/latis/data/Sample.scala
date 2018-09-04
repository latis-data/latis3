package latis.data

/**
 * Contain data for one sample of a Function.
 * The dimensionality is the number of Scalar data elements in the
 * domain of the Function. The rest of the "data" elements
 * represent the range. Range data can be ScalarData or
 * nested FunctionData. TupleData must be flattened
 * to yield only ScalarData or FunctionData. The model can be
 * used to recapture the tuple relations.
 */
case class Sample(dimensionality: Int, data: Array[Data]) {
  
  /**
   * Return the domain portion of this Sample as Data.
   */
  def domain: Data = dimensionality match {
    case 0 => ??? //TODO: Index(0) ?
    case 1 => data.head
    case n => TupleData(data.take(n): _*)
  }
  
  /**
   * Return the range portion of this Sample as Data.
   */
  def range: Data = data.length - dimensionality match {
    case 0 => ??? //TODO: not possible, no range variables
    case 1 => data.head
    case n => TupleData(data.drop(dimensionality): _*)
  }
}

object Sample {
  
  /**
   * Make a Sample from a single Data object.
   */
  def apply(data: Data): Sample = Sample(0, Array(data))
  
  //TODO: add convenience constructors

}
