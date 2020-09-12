package latis.model

import cats.syntax.all._
import scala.util.Try

import latis.data.Data._
import latis.data.Datum
import latis.data.NullDatum
import latis.util.LatisException

// Note, the parsing happens on the type side (as opposed to data)
// so special Scalar types can override.

//TODO: NumericType...?

sealed trait ValueType extends Serializable {
  def makeDatum(v: Any): Either[LatisException, Datum]
  def parseValue(value: String): Either[LatisException, Datum]
  def convertDouble(value: Double): Option[Datum] = None
  def fillValue: Datum = NullDatum
}

object BooleanValueType extends ValueType {
  def makeDatum(v: Any): Either[LatisException, Datum] = v match {
    case b: Boolean => Right(BooleanValue(b))
    case _ => Left(LatisException(s"Invalid BooleanValue: $v"))
  }

  def parseValue(value: String): Either[LatisException, Datum] = Try {
    BooleanValue(value.toBoolean)
  }.toEither.leftMap { t => LatisException(t.getMessage) }

  override def toString: String = "Boolean"
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

  override def toString: String = "Byte"
}

object CharValueType extends ValueType {
  def makeDatum(v: Any): Either[LatisException, Datum] = v match {
    case c: Char => Right(CharValue(c))
    case _ => Left(LatisException(s"Invalid CharValue: $v"))
  }

  def parseValue(value: String): Either[LatisException, Datum] = Try {
    CharValue(value.head)
  }.toEither.leftMap { t => LatisException(t.getMessage) }

  override def toString: String = "Char"
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

  override def toString: String = "Short"
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

  override def fillValue: IntValue = IntValue(Int.MaxValue)

  override def toString: String = "Int"
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

  override def fillValue: LongValue = LongValue(Long.MaxValue)

  override def toString: String = "Long"
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

  override def fillValue: FloatValue = FloatValue(Float.NaN)

  override def toString: String = "Float"
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

  override def fillValue: DoubleValue = DoubleValue(Double.NaN)

  override def toString: String = "Double"
}

object BinaryValueType extends ValueType {
  def makeDatum(v: Any): Either[LatisException, Datum] = v match {
    case a: Array[Byte] => Right(BinaryValue(a))
    case _ => Left(LatisException(s"Invalid BinaryValue: $v"))
  }

  def parseValue(value: String): Either[LatisException, Datum] = Try {
    ??? //TODO: uudecode?
  }.toEither.leftMap { t => LatisException(t.getMessage) }

  override def toString: String = "Binary"
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

  override def fillValue: StringValue = StringValue("")

  override def toString: String = "String"
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

  override def toString: String = "BigInt"
}

object BigDecimalValueType extends ValueType {
  def makeDatum(v: Any): Either[LatisException, Datum] = v match {
    case bd: BigDecimal => Right(BigDecimalValue(bd))
    case _ => Left(LatisException(s"Invalid BigDecimalValue: $v"))
  }

  def parseValue(value: String): Either[LatisException, Datum] = Try {
    BigDecimalValue(BigDecimal(value))
  }.toEither.leftMap { t => LatisException(t.getMessage) }

  override def convertDouble(value: Double): Option[Datum] =
    Some(BigDecimalValue(BigDecimal(value)))

  override def toString: String = "BigDecimal"
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
