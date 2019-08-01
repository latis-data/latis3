package latis.data

/**
 * The Data trait is the root of all data values that go into a Sample.
 */
trait Data extends Any
//TODO: seal? don't forget SampledFunction

/**
 * Define a trait for Data that can be ordered.
 * This is used to ensure that DomainData has an ordering.
 */
trait OrderedData extends Any with Data 
  // Note, can't make Ordering for any Data.
  // Only number and text makes sense to order.
//TODO: support equality even if we can't support ordering, see Curry

/**
 * Define a base trait for all numeric data.
 * Implementers of Number must be able to provide Float and Double
 * values. Note that, Integral provides Int and Long.
 * Pattern matching on Number with extract a Double value.
 * This does not include String representations of a numeric value.
 */
trait Number extends Any with OrderedData with Ordered[Number] {
  //TODO: include other numeric types?
  def toFloat: Float
  def toDouble: Double
}
object Number {
  // Extract a Double from a Number
  def unapply(data: Number): Option[Double] = Option(data.toDouble)
}

/**
 * Define a base trait for integral (integer) data.
 * Implementations of Integral must be able to provide
 * Int and Long values in addition to Float and Double
 * required by Number.
 * This enables specialized Integral types to match any
 * other Integral type.
 */
trait Integral extends Any with Number {
  def toInt: Int
  def toLong: Long
}

/**
 * Index is an Integral type which can be used in a
 * pattern match to extract an Int which is suitable for 
 * indexing a Seq.
 */
trait Index extends Any with Integral
object Index {
  def unapply(data: Integral): Option[Int] = Option(data.toInt)
}

/**
 * Integer is an Integral type which can be used in a
 * pattern match to extract a Long.
 */
trait Integer extends Any with Integral
object Integer {
  def unapply(data: Integral): Option[Long] = Option(data.toLong)
}

/**
 * Real is a Number type which can be used in a
 * pattern match to extract a Double.
 */
trait Real extends Any with Number
object Real {
  def unapply(data: Real): Option[Double] = Option(data.toDouble)
}

/**
 * Text is a type of Data whose value is represented as a String.
 * This trait also provide lexical ordering of Text Data.
 */
trait Text extends Any with OrderedData with Ordered[Text] {
  def stringValue: String
}
object Text {
  def unapply(data: Text): Option[String] = Option(data.stringValue)
}

/**
 * BinaryData is a type of Data that encapsulated a Byte Array.
 * There is no ordering provided for BinaryData.
 */
trait BinaryData extends Any with Data {
  def toBytes: Array[Byte]
}
object BinaryData {
  def unapply(data: BinaryData): Option[Array[Byte]] = Option(data.toBytes)
}


object Data {
    
  // Try to construct Data from anything
  def apply(thing: Any): Data = thing match {
    case x: Short  => ShortValue(x)
    case x: Int    => IntValue(x)
    case x: Long   => LongValue(x)
    case x: Float  => FloatValue(x)
    case x: Double => DoubleValue(x)
    case x: String => StringValue(x)
    //TODO: AnyData? ObjectData?
  }
  
  //Note, these are implicit so we can construct DomainData from primitive types
    
  //TODO: should we only make LongValue implicit for the Integral types?
  
  implicit class ShortValue(val value: Short) 
    extends AnyVal with Integral with Serializable {
      def toInt: Int = value.toInt
      def toLong: Long = value.toLong
      def toFloat: Float = value.toFloat
      def toDouble: Double = value.toDouble
      override def toString = value.toString
      def compare(that: Number): Int = 
        toDouble compare that.toDouble
  }
  
  implicit class IntValue(val value: Int) 
    extends AnyVal with Integral with Serializable {
      def toInt: Int = value
      def toLong: Long = value.toLong
      def toFloat: Float = value.toFloat
      def toDouble: Double = value.toDouble
      override def toString = value.toString
      def compare(that: Number): Int = 
        toDouble compare that.toDouble
  }
  
  implicit class LongValue(val value: Long) 
    extends AnyVal with Integral with Serializable {
      def toInt: Int = value.toInt
      def toLong: Long = value
      def toFloat: Float = value.toFloat
      def toDouble: Double = value.toDouble
      override def toString = value.toString
      def compare(that: Number): Int = 
        toDouble compare that.toDouble
  }
  
  implicit class FloatValue(val value: Float) 
    extends AnyVal with Real with Serializable {
      def toFloat: Float = value
      def toDouble: Double = value.toDouble
      override def toString = value.toString
      def compare(that: Number): Int = 
        toDouble compare that.toDouble
  }
  
  implicit class DoubleValue(val value: Double) 
    extends AnyVal with Real with Serializable {
      def toFloat: Float = value.toFloat
      def toDouble: Double = value
      override def toString = value.toString
      def compare(that: Number): Int = 
        toDouble compare that.toDouble
  }

  implicit class StringValue(val value: String) 
    extends AnyVal with Text with Serializable {
      def stringValue: String = value
      override def toString = value
      def compare(that: Text): Int = 
        stringValue compare that.stringValue
  }

  implicit class BinaryValue(val value: Array[Byte]) 
    extends AnyVal with BinaryData with Serializable {
      def toBytes: Array[Byte] = value
      override def toString = "BLOB"
  }

}