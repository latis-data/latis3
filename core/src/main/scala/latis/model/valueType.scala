package latis.model

import scala.annotation.nowarn

import cats.syntax.all._
import scala.util.Try

import latis.data.Data._
import latis.data.Datum
import latis.util.LatisException

/**
 * A ValueType represents the underlying Scala data type that is
 * wrapped as a Datum.
 */
sealed trait ValueType extends Serializable {
  /**
   * Tries to make data of this value type from any value.
   *
   * This requires that the value match the value type.
   * TODO: consider allowing any convertible type.
   */
  def makeDatum(value: Any): Either[LatisException, Datum]

  /**
   * Tries to interpret the given string value as this value type.
   *
   * This is used by Scalar.parseValue which should be used
   * in general for data parsing.
   */
  def parseValue(value: String): Either[LatisException, Datum]

  /**
   * Supports returning to this value type after unit conversion which assumes doubles.
   *
   * NaN will be converted to 0 for many value types.
   */
  def convertDouble(@nowarn("cat=unused") value: Double): Option[Datum] = None
  //TODO: beware silent truncation for overflow
  //  e.g. Double.MaxValue.toShort = -1
  //  Int, Long will yield their max value
  //TODO: consider fill value or null for invalid conversions
  //TODO: do in Scalar which has more context?
  //TODO: consider NumericType which can safely be used for math

}

object BooleanValueType extends ValueType {
  def makeDatum(v: Any): Either[LatisException, Datum] = v match {
    case b: Boolean => Right(BooleanValue(b))
    case _ => Left(LatisException(s"Invalid BooleanValue: $v"))
  }

  def parseValue(value: String): Either[LatisException, Datum] = Try {
    BooleanValue(value.toBoolean)
  }.toEither.leftMap { t => LatisException(t.getMessage) }

  override def toString: String = "boolean"
}

object ByteValueType extends ValueType {
  def makeDatum(v: Any): Either[LatisException, Datum] = v match {
    case b: Byte => Right(ByteValue(b))
    case _ => Left(LatisException(s"Invalid ByteValue: $v"))
  }

  def parseValue(value: String): Either[LatisException, Datum] = Try {
    ByteValue(value.toByte)
  }.toEither.leftMap { t => LatisException(t.getMessage) }

  //TODO: beware of overflow without error
  override def convertDouble(value: Double): Option[Datum] =
    Some(ByteValue(value.toByte))

  override def toString: String = "byte"
}

object CharValueType extends ValueType {
  def makeDatum(v: Any): Either[LatisException, Datum] = v match {
    case c: Char => Right(CharValue(c))
    case _ => Left(LatisException(s"Invalid CharValue: $v"))
  }

  def parseValue(value: String): Either[LatisException, Datum] = Try {
    CharValue(value.head)
  }.toEither.leftMap { t => LatisException(t.getMessage) }

  override def toString: String = "char"
}

object ShortValueType extends ValueType {
  def makeDatum(v: Any): Either[LatisException, Datum] = v match {
    case s: Short => Right(ShortValue(s))
    case _ => Left(LatisException(s"Invalid ShortValue: $v"))
  }

  def parseValue(value: String): Either[LatisException, Datum] = Try {
    ShortValue(value.toShort)
  }.toEither.leftMap { t => LatisException(t.getMessage) }

  //TODO: beware of overflow without error
  override def convertDouble(value: Double): Option[Datum] =
    Some(ShortValue(value.toShort))

  override def toString: String = "short"
}

object IntValueType extends ValueType {
  def makeDatum(v: Any): Either[LatisException, Datum] = v match {
    case i: Int => Right(IntValue(i))
    case _ => Left(LatisException(s"Invalid IntValue: $v"))
  }

  def parseValue(value: String): Either[LatisException, Datum] = Try {
    IntValue(value.toInt)
  }.toEither.leftMap { t => LatisException(t.getMessage) }

  //TODO: beware of overflow without error
  override def convertDouble(value: Double): Option[Datum] =
    Some(IntValue(value.toInt))

  override def toString: String = "int"
}

object LongValueType extends ValueType {
  def makeDatum(v: Any): Either[LatisException, Datum] = v match {
    case l: Long => Right(LongValue(l))
    case _ => Left(LatisException(s"Invalid LongValue: $v"))
  }

  def parseValue(value: String): Either[LatisException, Datum] = Try {
    LongValue(value.toLong)
  }.toEither.leftMap { t => LatisException(t.getMessage) }

  //TODO: beware of truncation or overflow without error
  override def convertDouble(value: Double): Option[Datum] =
    Some(LongValue(value.toLong))

  override def toString: String = "long"
}

object FloatValueType extends ValueType {
  def makeDatum(v: Any): Either[LatisException, Datum] = v match {
    case f: Float => Right(FloatValue(f))
    case _ => Left(LatisException(s"Invalid FloatValue: $v"))
  }

  def parseValue(value: String): Either[LatisException, Datum] = Try {
    FloatValue(value.toFloat)
  }.toEither.leftMap { t => LatisException(t.getMessage) }

  //TODO: beware of overflow to Infinity
  override def convertDouble(value: Double): Option[Datum] =
    Some(FloatValue(value.toFloat))

  override def toString: String = "float"
}

object DoubleValueType extends ValueType {
  def makeDatum(v: Any): Either[LatisException, Datum] = v match {
    //TODO: turn any numeric type into a Double?
    case d: Double => Right(DoubleValue(d))
    case _ => Left(LatisException(s"Invalid DoubleValue: $v"))
  }

  def parseValue(value: String): Either[LatisException, Datum] = Try {
    DoubleValue(value.toDouble)
  }.toEither.leftMap { t => LatisException(t.getMessage) }

  override def convertDouble(value: Double): Option[Datum] =
    Some(DoubleValue(value))

  override def toString: String = "double"
}

object BinaryValueType extends ValueType {
  def makeDatum(v: Any): Either[LatisException, Datum] = v match {
    case a: Array[Byte] => Right(BinaryValue(a))
    case _ => Left(LatisException(s"Invalid BinaryValue: $v"))
  }

  def parseValue(value: String): Either[LatisException, Datum] = Try {
    ??? //TODO: uudecode?
  }.toEither.leftMap { t => LatisException(t.getMessage) }

  override def toString: String = "binary"
}

object StringValueType extends ValueType {
  def makeDatum(v: Any): Either[LatisException, Datum] = v match {
    //TODO: turn any v to a string?
    case s: String => Right(StringValue(s))
    case _ => Left(LatisException(s"Invalid StringValue: $v"))
  }

  def parseValue(value: String): Either[LatisException, Datum] = Try {
    StringValue(value)
  }.toEither.leftMap { t => LatisException(t.getMessage) }

  override def toString: String = "string"
}

object BigIntValueType extends ValueType {
  def makeDatum(v: Any): Either[LatisException, Datum] = v match {
    case bi: BigInt => Right(BigIntValue(bi))
    case _ => Left(LatisException(s"Invalid BigIntValue: $v"))
  }

  def parseValue(value: String): Either[LatisException, Datum] = Try {
    BigIntValue(BigInt(value))
  }.toEither.leftMap { t => LatisException(t.getMessage) }

  //TODO: beware of truncation without error
  override def convertDouble(value: Double): Option[Datum] =
    Some(BigIntValue(BigInt(value.toLong)))

  override def toString: String = "bigInt"
}

object BigDecimalValueType extends ValueType {
  def makeDatum(v: Any): Either[LatisException, Datum] = v match {
    case bd: BigDecimal => Right(BigDecimalValue(bd))
    case _ => Left(LatisException(s"Invalid BigDecimalValue: $v"))
  }

  def parseValue(value: String): Either[LatisException, Datum] = Try {
    BigDecimalValue(BigDecimal(value))
  }.toEither.leftMap { t => LatisException(t.getMessage) }

  override def convertDouble(value: Double): Option[Datum] = {
    if (value.isNaN) None
    else Some(BigDecimalValue(BigDecimal(value)))
  }

  override def toString: String = "bigDecimal"
}

//ComplexValueType? could use Tuple, but arithmetic would fail;
//  separate concern that applies to all of these
//AnyValueType? could use custom Scalar subclass and binary encoding

object ValueType {

  def fromName(name: String): Either[LatisException, ValueType] = name.toLowerCase match {
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
    case s =>
      val msg = s"Invalid Scalar value type: $s"
      Left(LatisException(msg))
  }

  def fromValue(value: Any): Either[LatisException, ValueType] = value match {
    case _: Boolean     => Right(BooleanValueType)
    case _: Byte        => Right(ByteValueType)
    case _: Char        => Right(CharValueType)
    case _: Short       => Right(ShortValueType)
    case _: Int         => Right(IntValueType)
    case _: Long        => Right(LongValueType)
    case _: Float       => Right(FloatValueType)
    case _: Double      => Right(DoubleValueType)
    case _: Array[Byte] => Right(BinaryValueType) //will this work with erasure?, ByteBuffer?
    case _: String      => Right(StringValueType)
    case _: BigInt      => Right(BigIntValueType)
    case _: BigDecimal  => Right(BigDecimalValueType)
    case a =>
      val msg = s"Unsupported value type: ${a.getClass}}"
      Left(LatisException(msg))
  }
}
