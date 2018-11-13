package latis.data

/**
 * Define an Ordering to be applied for scalar data values.
 * Since the values can be of any type, this matches
 * a specific set of common types.
 * Note that this Ordering is designed to be applied among 
 * domain values of the same type (within a sequence of Samples
 * from a Dataset). It is not designed to compare arbitrary values
 * of different types.
 */
object ScalarOrdering extends Ordering[Any] {
  
  def compare(a: Any, b: Any) = (a, b) match {
    case (a: Boolean,    b: Boolean)    => a compare b
    case (a: Byte,       b: Byte)       => a compare b
    case (a: Char,       b: Char)       => a compare b
    case (a: Short,      b: Short)      => a compare b
    case (a: Int,        b: Int)        => a compare b
    case (a: Float,      b: Float)      => a compare b
    case (a: Long,       b: Long)       => a compare b
    case (a: Double,     b: Double)     => a compare b
    case (a: String,     b: String)     => a compare b
    case (a: BigInt,     b: BigInt)     => a compare b
    case (a: BigDecimal, b: BigDecimal) => a compare b
    //TODO: Numeric
    case _ => 
      val msg = s"Can't compare Scalar values: $a and $b"
      throw new UnsupportedOperationException(msg)
  }
}