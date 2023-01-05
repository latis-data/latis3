package latis.service.dap2

import java.net.URL
import java.nio.charset.StandardCharsets

import scodec._
import scodec.bits.BitVector

import latis.model._
import latis.util.LatisException
import latis.util.StringUtils._

sealed trait AtomicType[F] {
  private val fmtInt: AnyVal => String = i => i.toString
  private val fmtFloat: Double => String = f => f"$f%.6g"
  private val fmtString: String => String = ensureQuotedAndEscaped

  val vcodec: Codec[F]
  def asByteArray(value: F): Array[Byte] = {
    val pureBytes = vcodec.encode(value).require.toByteArray
    pureBytes.padTo(pureBytes.length + (4 - pureBytes.length % 4) % 4, 0x00.toByte)
  }
  def codec: Codec[Array[Byte]] = vcodec.xmap[Array[Byte]](
    v => asByteArray(v),
    arr => vcodec.decode(BitVector(arr)).require.value
  )

  def ofValue(value: F): F = value
  def asDasString(value: F): String = this.ofValue(value) match {
      case int: Byte => fmtInt(int)
      case int: Short => fmtInt(int)
      case int: Int => fmtInt(int)
      case int: Long => fmtInt(int)
      case float: Float => fmtFloat(float.toDouble)
      case float: Double => fmtFloat(float)
      case string: String => fmtString(string)
      case string: URL => fmtString(string.toString)
      case other => other.toString
  }
}

object AtomicType {
  final case object Byte extends AtomicType[Byte] {
    override val vcodec: Codec[Byte] = codecs.byte
  }
  final case object Int16 extends AtomicType[Short] {
    override val vcodec: Codec[Short] = codecs.short16
  }
  final case object UInt16 extends AtomicType[Int] {
    override val vcodec: Codec[Int] = codecs.uint16
    val max: Int = Integer.parseInt("FFFF",16)
    override def ofValue(value: Int): Int = value.min(max).max(0)
  }
  final case object Int32 extends AtomicType[Int] {
    override val vcodec: Codec[Int] = codecs.int32
  }
  final case object UInt32 extends AtomicType[Long] {
    override val vcodec: Codec[Long] = codecs.uint32
    val max: Long = java.lang.Long.parseLong("FFFFFFFF",16)
    override def ofValue(value: Long): Long = value.min(max).max(0)
  }
  final case object Float32 extends AtomicType[Float] {
    override val vcodec: Codec[Float] = codecs.float
  }
  final case object Float64 extends AtomicType[Double] {
    override val vcodec: Codec[Double] = codecs.double
  }
  final case object String extends AtomicType[String] {
    val vcodec: Codec[String] = codecs.string(StandardCharsets.UTF_8)
  }
  final case object Url extends AtomicType[URL] {
    override val vcodec: Codec[URL] = String.vcodec.xmap[URL](
      str => new URL(str),
      url => url.toString
    )
  }

  def fromScalar(scalar: Scalar): Either[LatisException, AtomicType[_]] = scalar.valueType match {
    case DoubleValueType => Right(Float64)
    case FloatValueType => Right(Float32)
    case IntValueType => Right(Int32)
    case ShortValueType => Right(Int16)
    case ByteValueType => Right(Byte)
    case StringValueType => Right(String)
    case _ => Left(LatisException("Scalar could not be parsed to a DDS atomic type."))
  }
}
