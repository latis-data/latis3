package latis.data

/**
 * Define a Number object that can be used to pattern match and extract 
 * a Double from a data value. If the value can't be expresses as a Double 
 * then the extracted value will be NaN.
 */
object Number {
  
  def unapply(value: Any): Option[Double] = value match {
    case true          => Some(1.0)
    case false         => Some(0.0)
    case v: Char       => Some(v.toDouble)
    case v: Short      => Some(v.toDouble)
    case v: Int        => Some(v.toDouble)
    case v: Long       => Some(v.toDouble)
    case v: Float      => Some(v.toDouble)
    case v: Double     => Some(v.toDouble)
    case v: BigInt     => Some(v.toDouble)
    case v: BigDecimal => Some(v.toDouble)
    case v: String     => Some(stringToDouble(v))
    case _             => Some(Double.NaN)
  }
  
  def stringToDouble(s: String): Double = {
    try { s.toDouble } 
    catch { case _: NumberFormatException => Double.NaN }
  }
}