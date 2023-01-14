package latis.service.dap2

import java.net.URL

import latis.data.Data
import latis.data.Data._
import latis.data.Datum
import latis.util.LatisException
import latis.util.StringUtils.ensureQuotedAndEscaped

sealed trait AtomicTypeValue[+T] {
  val value: T
  val dasStr: String
}
object AtomicTypeValue {
  private val fmtInt: AnyVal => String = i => i.toString
  private val fmtFloat: Double => String = f => f"$f%.6g"
  private val fmtString: String => String = ensureQuotedAndEscaped

  final case class ByteValue(override val value: Byte) extends AtomicTypeValue[Byte] {
    override val dasStr: String = fmtInt(value)
  }
  final case class Int16Value(override val value: Short) extends AtomicTypeValue[Short] {
    override val dasStr: String = fmtInt(value)
  }
  final case class UInt16Value(_value: Int) extends AtomicTypeValue[Int] {
    val max: Int = Integer.parseInt("FFFF", 16)
    override val value: Int = _value.min(max).max(0)
    override val dasStr: String = fmtInt(value)
  }
  final case class Int32Value(value: Int) extends AtomicTypeValue[Int] {
    override val dasStr: String = fmtInt(value)
  }
  final case class UInt32Value(_value: Long) extends AtomicTypeValue[Long] {
    val max: Long = java.lang.Long.parseLong("FFFFFFFF", 16)
    override val value: Long = _value.min(max).max(0)
    override val dasStr: String = fmtInt(value)
  }
  final case class Float32Value(override val value: Float) extends AtomicTypeValue[Float] {
    override val dasStr: String = fmtFloat(value.toDouble)
  }
  final case class Float64Value(override val value: Double) extends AtomicTypeValue[Double] {
    override val dasStr: String = fmtFloat(value)
  }
  final case class StringValue(override val value: String) extends AtomicTypeValue[String] {
    override val dasStr: String = fmtString(value)
  }
  final case class UrlValue(override val value: URL) extends AtomicTypeValue[URL] {
    override val dasStr: String = fmtString(value.toString)
  }

  def fromDatum(datum: Datum): Either[LatisException, AtomicTypeValue[_]] = datum match {
    case double: DoubleValue => Right(Float64Value(double.value))
    case float: FloatValue => Right(Float32Value(float.value))
    case int: IntValue => Right(Int32Value(int.value))
    case short: ShortValue => Right(Int16Value(short.value))
    case byte: Data.ByteValue => Right(ByteValue(byte.value))
    case str: Data.StringValue => Right(StringValue(str.value))
    case _ => Left(LatisException("Datum could not be parsed to a DDS atomic type."))
  }
}
