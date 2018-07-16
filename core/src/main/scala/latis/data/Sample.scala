package latis.data

/**
 * Contain data for one sample of a Function.
 * The arity is the number of (Scalar) data elements in the
 * domain of the Function. The rest of the "data" elements
 * represent the range. Range data can be ScalarData or
 * nested FunctionData. TupleData must be flattened
 * to yield only ScalarData or FunctionData. The model can be
 * used to recapture the tuple relations.
 */
case class Sample(arity: Int, data: Array[Data])

object Sample {
  
  /**
   * Make a Sample from a single Data object.
   */
  def apply(data: Data): Sample = Sample(0, Array(data))

}
