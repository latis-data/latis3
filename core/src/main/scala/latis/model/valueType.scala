package latis.model

import latis.data.Datum
import latis.data.Data._
import latis.data.NullData

import scala.util.Try

// Note, the parsing happens on the type side (as opposed to data)
// so special Scalar types can override.

//TODO: NumericType...?

sealed trait ValueType {
  def parseValue(value: String): Try[Datum]
  def convertDouble(value: Double): Option[Datum] = None
  def fillValue: Datum = NullData
}


object BooleanValueType extends ValueType {
  def parseValue(value: String): Try[Datum] = Try {
    BooleanValue(value.toBoolean)
  }
}

object ByteValueType extends ValueType {
  def parseValue(value: String): Try[Datum] = Try {
    ByteValue(value.toByte)
  }
  //TODO: beware of overflow without error
  override def convertDouble(value: Double): Option[Datum] =
    Some(ByteValue(value.toByte))
}

object CharValueType extends ValueType {
  def parseValue(value: String): Try[Datum] = Try {
    CharValue(value.head)
  }
}

object ShortValueType extends ValueType {
  def parseValue(value: String): Try[Datum] = Try {
    ShortValue(value.toShort)
  }
  //TODO: beware of overflow without error
  override def convertDouble(value: Double): Option[Datum] =
    Some(ShortValue(value.toShort))
}

object IntValueType extends ValueType {
  def parseValue(value: String): Try[Datum] = Try {
    IntValue(value.toInt)
  }
  //TODO: beware of overflow without error
  override def convertDouble(value: Double): Option[Datum] =
    Some(IntValue(value.toInt))
  override def fillValue: IntValue = IntValue(Int.MinValue)
}

object LongValueType extends ValueType {
  def parseValue(value: String): Try[Datum] = Try {
    LongValue(value.toLong)
  }
  //TODO: beware of truncation or overflow without error
  override def convertDouble(value: Double): Option[Datum] =
    Some(LongValue(value.toLong))
  override def fillValue: LongValue = LongValue(Long.MinValue)
}

object FloatValueType extends ValueType {
  def parseValue(value: String): Try[Datum] = Try {
    FloatValue(value.toFloat)
  }
  //TODO: beware of overflow to Infinity
  override def convertDouble(value: Double): Option[Datum] =
    Some(FloatValue(value.toFloat))
  override def fillValue: FloatValue = FloatValue(Float.NaN)
}

object DoubleValueType extends ValueType {
  def parseValue(value: String): Try[Datum] = Try {
    DoubleValue(value.toDouble)
  }
  override def convertDouble(value: Double): Option[Datum] =
    Some(DoubleValue(value))
  override def fillValue: DoubleValue = DoubleValue(Double.NaN)
}

object BinaryValueType extends ValueType {
  def parseValue(value: String): Try[Datum] = Try {
    ??? //TODO: uudecode?
  }
}

object StringValueType extends ValueType {
  def parseValue(value: String): Try[Datum] = Try {
    StringValue(value)
  }
  override def fillValue: StringValue = StringValue("")
}

object BigIntValueType extends ValueType {
  def parseValue(value: String): Try[Datum] = Try {
    BigIntValue(BigInt(value))
  }
  //TODO: beware of truncation without error
  override def convertDouble(value: Double): Option[Datum] =
    Some(BigIntValue(BigInt(value.toLong)))
}

object BigDecimalValueType extends ValueType {
  def parseValue(value: String): Try[Datum] = Try {
    BigDecimalValue(BigDecimal(value))
  }
  override def convertDouble(value: Double): Option[Datum] =
    Some(BigDecimalValue(BigDecimal(value)))
}


//ComplexValueType? could use Tuple, but arithmetic would fail;
//  separate concern that applies to all of these
//AnyValueType? could use custom Scalar subclass and binary encoding

object ValueType {

  def fromName(name: String): Either[Exception, ValueType] = name.toLowerCase match {
    case "boolean"    => Right(BooleanValueType)
    case "byte"       => Right(ByteValueType)
    case "char"       => Right(CharValueType)
    case "short"      => Right(ShortValueType)
    case "int"        => Right(IntValueType)
    case "long"       => Right(LongValueType)
    case "float"      => Right(FloatValueType)
    case "double"     => Right(DoubleValueType)
    case "binary"     => Right(BinaryValueType)
    case "string"     => Right(StringValueType)
    case "bigint"     => Right(BigIntValueType)
    case "bigdecimal" => Right(BigDecimalValueType)
    case s            =>
      val msg = s"Invalid Scalar value type: $s"
      Left(new RuntimeException(msg))
  }

}
