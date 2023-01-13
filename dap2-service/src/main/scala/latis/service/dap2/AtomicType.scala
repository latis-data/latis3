package latis.service.dap2

import java.net.URL
import java.nio.charset.StandardCharsets

import scodec._
import scodec.bits.BitVector
import scodec.bits.ByteVector

import latis.data.Data
import latis.data.Data._
import latis.data.Datum
import latis.model._
import latis.util.LatisException
import latis.util.StringUtils._

sealed trait AtomicType[F] {
  private val fmtInt: AnyVal => String = i => i.toString
  private val fmtFloat: Double => String = f => f"$f%.6g"
  private val fmtString: String => String = ensureQuotedAndEscaped
  val vcodec: Codec[F]

  protected def ofValue(value: F): F = value
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

  def padBytes(pureBytes: Array[Byte]): Array[Byte] =
    pureBytes.padTo(pureBytes.length + (4 - pureBytes.length % 4) % 4, 0x00.toByte)

  def unpadBytes(paddedBytes: Array[Byte]): Array[Byte] = paddedBytes

  val bcodec = codecs.bytes.xmap[Array[Byte]](
    bv => unpadBytes(bv.toArray),
    arr => ByteVector(padBytes(arr))
  )

  def encode(value: F): Either[LatisException, Array[Byte]] = vcodec
    .encode(this.ofValue(value))
    .fold(err => Left(LatisException(err.message)), bv => Right(bv.toByteArray))
    .map { arr => bcodec.encode(arr)
        .fold(err => Left(LatisException(err.message)), bv => Right(bv.toByteArray))
    }.flatten

  def decode(bytes: Array[Byte]): Either[LatisException, F] = bcodec
    .decode(BitVector(bytes))
    .fold(err => Left(LatisException(err.message)), arr => Right(arr.value))
    .map { arr => vcodec.decode(BitVector(arr))
      .fold(err => Left(LatisException(err.message)), value => Right(value.value))
    }.flatten

  def codec: Codec[F] = bcodec.xmap[F](
    bytes => vcodec.decode(BitVector(bytes)).getOrElse(return codecs.fail(Err(""))).value,
    value => vcodec.encode(value).getOrElse(return codecs.fail(Err(""))).toByteArray
  )
}

object AtomicType {
  final case object Byte extends AtomicType[Byte] {
    override val vcodec: Codec[Byte] = codecs.byte
    override def unpadBytes(paddedBytes: Array[Byte]): Array[Byte] = paddedBytes.slice(0, 1)
  }
  final case object Int16 extends AtomicType[Short] {
    override val vcodec: Codec[Short] = codecs.short16
    override def unpadBytes(paddedBytes: Array[Byte]): Array[Byte] = paddedBytes.slice(0, 2)
  }
  final case object UInt16 extends AtomicType[Int] {
    override val vcodec: Codec[Int] = codecs.uint16
    val max: Int = Integer.parseInt("FFFF",16)
    override protected def ofValue(value: Int): Int = value.min(max).max(0)
  }
  final case object Int32 extends AtomicType[Int] {
    override val vcodec: Codec[Int] = codecs.int32
  }
  final case object UInt32 extends AtomicType[Long] {
    override val vcodec: Codec[Long] = codecs.uint32
    val max: Long = java.lang.Long.parseLong("FFFFFFFF",16)
    override protected def ofValue(value: Long): Long = value.min(max).max(0)
  }
  final case object Float32 extends AtomicType[Float] {
    override val vcodec: Codec[Float] = codecs.float
  }
  final case object Float64 extends AtomicType[Double] {
    override val vcodec: Codec[Double] = codecs.double
  }
  final case object String extends AtomicType[String] {
    override val vcodec: Codec[String] = codecs.string(StandardCharsets.UTF_8)
    override def unpadBytes(paddedBytes: Array[Byte]): Array[Byte] = {
      val len = paddedBytes.length
      val suffix = paddedBytes.indexOf(0x00.toByte)
      paddedBytes.slice(0, if (suffix > 0) suffix else len)
    }
  }
  final case object Url extends AtomicType[URL] {
    override val vcodec: Codec[URL] = codecs.string(StandardCharsets.UTF_8)
      .xmap[URL](str => new URL(str), url => url.toString)

    override def unpadBytes(paddedBytes: Array[Byte]): Array[Byte] = {
      val len = paddedBytes.length
      val suffix = paddedBytes.indexOf(0x00.toByte)
      paddedBytes.slice(0, if (suffix > 0) suffix else len)
    }
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

  private def codecFromType[F,D<:Datum](at: AtomicType[F], data: F => D): Codec[D] = at.bcodec.xmap[D](
    arr => at.decode(arr).map(data).getOrElse(return codecs.fail(Err("Failed to build Datum decoder"))),
    v => at.encode(v.value.asInstanceOf[F]).getOrElse(return codecs.fail(Err("Failed to build Datum encoder")))
  )

  def dataCodec(scalar: Scalar): Codec[Data] = scalar.valueType match {
    case DoubleValueType => codecFromType(Float64, DoubleValue).upcast[Data]
    case FloatValueType => codecFromType(Float32, FloatValue).upcast[Data]
    case IntValueType => codecFromType(Int32, IntValue).upcast[Data]
    case ShortValueType => codecFromType(Int16, ShortValue).upcast[Data]
    case ByteValueType => codecFromType(Byte, ByteValue).upcast[Data]
    case StringValueType => codecFromType(String, StringValue).upcast[Data]
    case _ => codecs.fail(Err("Scalar could not be parsed to a DDS atomic type."))
  }

  def encodeDatum(datum: Datum): Either[LatisException, Array[Byte]] = datum match {
    case double: DoubleValue => Float64.encode(double.value)
    case float: FloatValue => Float32.encode(float.value)
    case int: IntValue => Int32.encode(int.value)
    case short: ShortValue => Int16.encode(short.value)
    case byte: ByteValue => Byte.encode(byte.value)
    case str: StringValue => String.encode(str.value)
    case _ => Left(LatisException("Datum could not be parsed to a DDS atomic type."))
  }
}
