package latis.data

/**
 * The Data trait is the root of all data values that go into a Sample.
 */
trait Data extends Any
//TODO: seal? don't forget SampledFunction

/*
 * Note: traits used for two purposes:
 *
 * - mixin properties
 * For example, Integer, which must be able to provide a Long,
 * is-a Number which must be able to provide a Double.
 * Thus, any data that is a subtype of Integer must be able to
 * provide both Long or Double.
 *
 * - pattern matching
 * For example, a value created as a subtype of Integer will match
 * Number (to extract a Double) or Integer (to extract a Long) but
 * won't match Real. Likewise, a Real can match Number but not Integer.
 * If you simply want to consume a data value as a number, match
 * on Number and use the Double value.
 * Neither would match Text even if the string value is "numeric".
 */

trait Datum extends Any with Data {
  def asString: String
}

// Used for default fillValue
object NullData extends Datum {
  def asString: String = "null"
}

/**
 * Define a base trait for all numeric data.
 * Implementers of Number must be able to provide values as various
 * primitive numeric types.
 * Pattern matching on Number will extract a Double value.
 * This does not include String representations of a numeric value.
 */
trait Number extends Any with Datum {
  def asDouble: Double
  def asString: String = asDouble.toString
}
object Number {
  // Extract a Double from a Number
  def unapply(data: Number): Option[Double] = Option(data.asDouble)
}

/**
 * Integer is a Number type representing integral data.
 * Pattern matching on Integer will extract a Long.
 */
trait Integer extends Any with Number {
  def asLong: Long
  def asDouble: Double = asLong.toDouble
  override def asString: String = asLong.toString
}
object Integer {
  // Extract a Long from an Integer
  def unapply(data: Integer): Option[Long] = Option(data.asLong)
}

/**
 * Index is an Integer type representing data that can be used
 * to index a Seq or Array. Pattern matching on Index will
 * extract an Int.
 */
trait Index extends Any with Integer {
  def asInt: Int
  def asLong: Long = asInt.toLong
  override def asDouble: Double = asInt.toDouble
  override def asString: String = asInt.toString
}
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

/**
 * Text is a type of Data whose value is represented as a String.
 */
trait Text extends Any with Datum
object Text {
  // Extract a String from a Text
  def unapply(data: Text): Option[String] = Option(data.asString)
}

//=============================================================================

// Import latis.data.Data._ to get implicit Data construction from supported types
object Data {

  /**
   * Construct Data from anything.
   */
  //TODO: don't need with implicit construtors?
//  def apply(thing: Any): Data = thing match {
//    case x: Short       => ShortValue(x)
//    case x: Int         => IntValue(x)
//    case x: Long        => LongValue(x)
//    case x: Float       => FloatValue(x)
//    case x: Double      => DoubleValue(x)
//    case x: String      => StringValue(x)
//    case x: Array[Byte] => BinaryValue(x)
//    //TODO: boolean, byte, char
//    //TODO: BigInt, BigDecimal
//    //TODO: AnyData? ObjectData? should be Serializable
//  }

  //Note, these are value classes
  //Note, these are implicit so we can construct DomainData from primitive types

  implicit class BooleanValue(val value: Boolean) extends AnyVal with Datum with Serializable {
    def asString: String = value.toString
    override def toString = s"BooleanValue($value)"
  }

  implicit class ByteValue(val value: Byte) extends AnyVal with Index with Serializable {
    def asInt: Int = value.toInt
    override def toString = s"ByteValue($value)"
  }

  // single Text character or Integer 0-65535
  implicit class CharValue(val value: Char) extends AnyVal with Text with Index with Serializable {
    def asInt: Int = value.toInt
    override def toString = s"CharValue($value)"
    //TODO: numeric or single test character?
  }

  implicit class ShortValue(val value: Short) extends AnyVal with Index with Serializable {
    def asInt: Int = value.toInt
    override def toString = s"ShortValue($value)"
  }

  implicit class IntValue(val value: Int) extends AnyVal with Index with Serializable {
    def asInt: Int = value
    override def toString = s"IntValue($value)"
  }

  implicit class LongValue(val value: Long) extends AnyVal with Integer with Serializable {
    def asLong: Long = value
    override def toString = s"LongValue($value)"
  }

  implicit class FloatValue(val value: Float) extends AnyVal with Real with Serializable {
    def asDouble: Double = value.toDouble
    override def toString = s"FloatValue($value)"
  }

  implicit class DoubleValue(val value: Double) extends AnyVal with Real with Serializable {
    def asDouble: Double = value
    override def toString = s"DoubleValue($value)"
  }

  implicit class StringValue(val value: String) extends AnyVal with Text with Serializable {
    def asString: String = value
    override def toString = s"StringValue($asString)"
  }

  implicit class BinaryValue(val value: Array[Byte]) extends AnyVal with Datum with Serializable {
    def asString: String = "BLOB"
    override def toString = s"BinaryValue($asString)"
  }

  implicit class BigIntValue(val value: BigInt) extends AnyVal with Integer with Serializable {
    def asLong: Long = value.toLong //won't break but may be wrong
    override def toString = s"BigIntValue($value)"
  }

  implicit class BigDecimalValue(val value: BigDecimal) extends AnyVal with Real with Serializable {
    def asDouble: Double = value.toDouble //may lose precision
    override def toString = s"BigDecimalValue($value)"
  }
}
