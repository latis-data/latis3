package latis.output

import scodec._
import scodec.bits._

import latis.data.Data
import latis.data.Data._
import latis.model._

object DataCodec {

  /** Instance of scodec.Codec for Data. */
  def defaultDataCodec(s: Scalar): Codec[Data] = s.valueType match {
    case BooleanValueType => codecs.bool(8).xmap[BooleanValue](BooleanValue, _.value).upcast[Data]
    case ByteValueType    => codecs.byte.xmap[ByteValue](ByteValue, _.value).upcast[Data]
    case ShortValueType   => codecs.short16.xmap[ShortValue](ShortValue, _.value).upcast[Data]
    case IntValueType     => codecs.int32.xmap[IntValue](IntValue, _.value).upcast[Data]
    case LongValueType    => codecs.int64.xmap[LongValue](LongValue, _.value).upcast[Data]
    case FloatValueType   => codecs.float.xmap[FloatValue](FloatValue, _.value).upcast[Data]
    case DoubleValueType  => codecs.double.xmap[DoubleValue](DoubleValue, _.value).upcast[Data]
    case StringValueType if s.metadata.getProperty("size").nonEmpty =>
      val size = s.metadata.getProperty("size").get.toLong * 8
      codecs.paddedFixedSizeBits(
        size,
        codecs.utf8,
        codecs.literals.constantBitVectorCodec(BitVector(hex"00"))
      ).xmap[StringValue](s => StringValue(s.replace("\u0000", "")), _.value).upcast[Data]
    case StringValueType  => codecs.fail(Err("BinaryEncoder does not support String without a size defined."))
    case BinaryValueType  =>
      codecs.bytes.xmap[BinaryValue](
        bytVec => BinaryValue(bytVec.toArray),
        binVal => ByteVector(binVal.value)
      ).upcast[Data]
    //Note that there is no standard binary encoding for Char, BigInt, or BigDecimal
    case CharValueType       => codecs.fail(Err("BinaryEncoder does not support Char."))
    case BigIntValueType     => codecs.fail(Err("BinaryEncoder does not support BigInt."))
    case BigDecimalValueType => codecs.fail(Err("BinaryEncoder does not support BigDecimal."))
  }

}
