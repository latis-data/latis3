package latis.service.dap2

import java.net.URL
import java.nio.charset.StandardCharsets

import scodec._
import scodec.bits.BitVector
import scodec.bits.ByteVector

import latis.model._
import latis.service.dap2.AtomicTypeValue._
import latis.util.LatisException

sealed trait AtomicType[F<:AtomicTypeValue[_]] {
  protected val vcodec: Codec[F]
  protected def removePad(paddedBytes: Array[Byte]): Array[Byte] = paddedBytes
  private def addPad(pureBytes: Array[Byte]): Array[Byte] =
    pureBytes.padTo(pureBytes.length + (4 - pureBytes.length % 4) % 4, 0x00.toByte)
  private val padCodec = codecs.bytes.xmap[Array[Byte]](
    bv => removePad(bv.toArray),
    arr => ByteVector(addPad(arr))
  )

  final def codec: Codec[F] = padCodec.xmap[F](
    bytes => vcodec.decode(BitVector(bytes)).getOrElse(
      return codecs.fail(Err("Value codec decoding failed"))
    ).value,
    value => vcodec.encode(value).getOrElse(
      return codecs.fail(Err("Value codec encoding failed"))
    ).toByteArray
  )
}

object AtomicType {
  final case object Byte extends AtomicType[ByteValue] {
    override protected val vcodec: Codec[ByteValue] = codecs.byte.xmap(ByteValue, _.value)
    override protected def removePad(paddedBytes: Array[Byte]): Array[Byte] = paddedBytes.slice(0, 1)
  }
  final case object Int16 extends AtomicType[Int16Value] {
    override protected val vcodec: Codec[Int16Value] = codecs.short16.xmap(Int16Value, _.value)
    override protected def removePad(paddedBytes: Array[Byte]): Array[Byte] = paddedBytes.slice(0, 2)
  }
  final case object UInt16 extends AtomicType[UInt16Value] {
    override protected val vcodec: Codec[UInt16Value] = codecs.uint16.xmap(UInt16Value, _.value)
  }
  final case object Int32 extends AtomicType[Int32Value] {
    override protected val vcodec: Codec[Int32Value] = codecs.int32.xmap(Int32Value, _.value)
  }
  final case object UInt32 extends AtomicType[UInt32Value] {
    override protected val vcodec: Codec[UInt32Value] = codecs.uint32.xmap(UInt32Value, _.value)
  }
  final case object Float32 extends AtomicType[Float32Value] {
    override protected val vcodec: Codec[Float32Value] = codecs.float.xmap(Float32Value, _.value)
  }
  final case object Float64 extends AtomicType[Float64Value] {
    override protected val vcodec: Codec[Float64Value] = codecs.double.xmap(Float64Value, _.value)
  }
  final case object String extends AtomicType[StringValue] {
    override protected val vcodec: Codec[StringValue] = codecs.string(StandardCharsets.UTF_8).xmap(StringValue, _.value)
    override protected def removePad(paddedBytes: Array[Byte]): Array[Byte] = {
      val len = paddedBytes.length
      val suffix = paddedBytes.indexOf(0x00.toByte)
      paddedBytes.slice(0, if (suffix > 0) suffix else len)
    }
  }
  final case object Url extends AtomicType[UrlValue] {
    override protected val vcodec: Codec[UrlValue] = codecs.string(StandardCharsets.UTF_8)
      .xmap[URL](str => new URL(str), url => url.toString)
      .xmap(UrlValue, _.value)

    override protected def removePad(paddedBytes: Array[Byte]): Array[Byte] = {
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
}
