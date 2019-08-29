package latis.data

/**
 * The Data trait is the root of all data values that go into a Sample.
 */
trait Data extends Any
//TODO: seal? don't forget SampledFunction

/**
 * Define a base trait for all numeric data.
 * Implementers of Number must be able to provide values as various 
 * primitive numeric types.
 * Pattern matching on Number will extract a Double value.
 * This does not include String representations of a numeric value.
 */
trait Number extends Any with Data {
  //TODO: include other numeric types?
  def asInt: Int
  def asLong: Long
  def asFloat: Float
  def asDouble: Double
  
  def asString: String
}
object Number {
  // Extract a Double from a Number
  def unapply(data: Number): Option[Double] = Option(data.asDouble)
}

/**
 * Integer is a Number type representing integral data.
 * Pattern matching on Integer will extract a Long.
 */
trait Integer extends Any with Number
object Integer {
  // Extract a Long from an Integer
  def unapply(data: Integer): Option[Long] = Option(data.asLong)
}

/**
 * Index is an Integer type representing data that can be used
 * to index a Seq or Array. Pattern matching on Index will
 * extract an Int.
 */
trait Index extends Any with Integer
object Index {
  // Extract an Int from an Index
  def unapply(data: Index): Option[Int] = Option(data.asInt)
}

/**
 * Real is a Number type which can be used in a
 * pattern match to extract a Double.
 */
trait Real extends Any with Number
object Real {
  // Extract a Double from a Real
  def unapply(data: Real): Option[Double] = Option(data.asDouble)
}

//TODO: Boolean
//TODO: Complex

/**
 * Text is a type of Data whose value is represented as a String.
 */
trait Text extends Any with Data {
  def asString: String
}
object Text {
  // Extract a String from a Text
  def unapply(data: Text): Option[String] = Option(data.asString)
}

/**
 * BinaryData is a type of Data that represents a binary blob.
 * A pattern match on BinaryData will extract a Byte Array.
 */
trait BinaryData extends Any with Data {
  def asBytes: Array[Byte]
}
object BinaryData {
  // Extract a Byte Array from a BinaryData
  def unapply(data: BinaryData): Option[Array[Byte]] = Option(data.asBytes)
}

//=============================================================================

object Data {
    
  /**
   * Construct Data from anything.
   */
  def apply(thing: Any): Data = thing match {
    case x: Short       => ShortValue(x)
    case x: Int         => IntValue(x)
    case x: Long        => LongValue(x)
    case x: Float       => FloatValue(x)
    case x: Double      => DoubleValue(x)
    case x: String      => StringValue(x)
    case x: Array[Byte] => BinaryValue(x)
    //TODO: boolean, byte, char
    //TODO: BigInt, BigDecimal
    //TODO: AnyData? ObjectData? should be Serializable
  }
  
  //Note, these are value classes
  //Note, these are implicit so we can construct DomainData from primitive types
  
  implicit class ShortValue(val value: Short) 
    extends AnyVal with Index with Serializable {
      def asInt: Int = value.toInt
      def asLong: Long = value.toLong
      def asFloat: Float = value.toFloat
      def asDouble: Double = value.toDouble
      def asString: String = value.toString
  }
  
  implicit class IntValue(val value: Int) 
    extends AnyVal with Index with Serializable {
      def asInt: Int = value
      def asLong: Long = value.toLong
      def asFloat: Float = value.toFloat
      def asDouble: Double = value.toDouble
      def asString: String = value.toString
  }
  
  implicit class LongValue(val value: Long) 
    extends AnyVal with Integer with Serializable {
      def asInt: Int = value.toInt
      def asLong: Long = value
      def asFloat: Float = value.toFloat
      def asDouble: Double = value.toDouble
      def asString: String = value.toString
  }
  
  implicit class FloatValue(val value: Float) 
    extends AnyVal with Real with Serializable {
      def asInt: Int = value.toInt
      def asLong: Long = value.toLong
      def asFloat: Float = value
      def asDouble: Double = value.toDouble
      def asString: String = value.toString
  }
  
  implicit class DoubleValue(val value: Double) 
    extends AnyVal with Real with Serializable {
      def asInt: Int = value.toInt
      def asLong: Long = value.toLong
      def asFloat: Float = value.toFloat
      def asDouble: Double = value
      def asString: String = value.toString
  }

  implicit class StringValue(val value: String) 
    extends AnyVal with Text with Serializable {
      def asString: String = value
  }

  implicit class BinaryValue(val value: Array[Byte]) 
    extends AnyVal with BinaryData with Serializable {
      def asBytes: Array[Byte] = value
      def asString: String = "BLOB"
  }

}
