package latis.output

import cats.effect.IO
import fs2.Stream
import scodec.{Encoder => SEncoder, _}
import scodec.Codec
import scodec.bits._
import scodec.stream.StreamEncoder

import latis.data._
import latis.data.Data._
import latis.dataset._
import latis.model._
import latis.ops.Uncurry

class BinaryEncoder extends Encoder[IO, BitVector] {
  //TODO: deal with NullData, require replaceMissing?

  /**
   * Encodes the Stream of Samples from the given Dataset as a Stream
   * of BitVectors.
   */
  override def encode(dataset: Dataset): Stream[IO, BitVector] = {
    val uncurriedDataset = dataset.withOperation(Uncurry())
    // Encode each Sample as a BitVector in the Stream
    sampleStreamEncoder(uncurriedDataset.model).encode(uncurriedDataset.samples)
  }

  /** Instance of scodec.stream.StreamEncoder for Sample. */
  def sampleStreamEncoder(model: DataType): StreamEncoder[Sample] =
    StreamEncoder.many(sampleEncoder(model))

  def sampleEncoder(model: DataType): SEncoder[Sample] = new SEncoder[(DomainData, RangeData)] {

    def encode(sample: (DomainData, RangeData)): Attempt[BitVector] = {
      // Note that the dataset has been uncurried so there are no nested functions.
      val scalars: List[Scalar] = model.getScalars.filterNot(_.isInstanceOf[Index])
      val datas: List[Data] = sample.domain ++ sample.range
      //TODO: assert that the lengths are the same? should be ensured earlier
      scalars.zip(datas).foldLeft(Attempt.successful(BitVector(hex""))) {
        case (ab: Attempt[BitVector], (s: Scalar, d: Data)) =>
          ab.flatMap { b =>
            dataCodec(s).encode(d).map(b ++ _)
          }
      }
    }

    def sizeBound: SizeBound = SizeBound.unknown
  }

  /** Instance of scodec.Codec for Data. */
  def dataCodec(s: Scalar): Codec[Data] = s.valueType match {
    case BooleanValueType => codecs.bool(8).xmap[BooleanValue](BooleanValue(_), _.value).upcast[Data]
    case ByteValueType    => codecs.byte.xmap[ByteValue](ByteValue(_), _.value).upcast[Data]
    case ShortValueType   => codecs.short16.xmap[ShortValue](ShortValue(_), _.value).upcast[Data]
    case IntValueType     => codecs.int32.xmap[IntValue](IntValue(_), _.value).upcast[Data]
    case LongValueType    => codecs.int64.xmap[LongValue](LongValue(_), _.value).upcast[Data]
    case FloatValueType   => codecs.float.xmap[FloatValue](FloatValue(_), _.value).upcast[Data]
    case DoubleValueType  => codecs.double.xmap[DoubleValue](DoubleValue(_), _.value).upcast[Data]
    case StringValueType if (s.metadata.getProperty("size").nonEmpty) =>
      val size = s.metadata.getProperty("size").get.toLong * 8
      codecs.paddedFixedSizeBits(
        size,
        codecs.utf8,
        codecs.literals.constantBitVectorCodec(BitVector(hex"00"))
      ).xmap[StringValue](StringValue(_), _.value).upcast[Data]
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
