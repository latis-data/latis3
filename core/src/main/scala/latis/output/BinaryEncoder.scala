package latis.output

import cats.effect.IO
import cats.syntax.all._
import fs2.Stream
import scodec.Codec
import scodec.bits._
import scodec.interop.cats._
import scodec.stream.StreamEncoder
import scodec.{Encoder => _, _}

import latis.data.Data._
import latis.data._
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
    StreamEncoder.many(sampleCodec(model))

  /** Instance of scodec.Codec for Data. */
  def dataCodec(s: Scalar): Codec[Data] = s.valueType match {
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

  private def codecOfList[A](cs: List[Codec[A]]): Codec[List[A]] = new Codec[List[A]] {

    override def sizeBound: SizeBound =
      cs.map(_.sizeBound).foldLeft(SizeBound.exact(0))(_ + _)

    override def decode(bits: BitVector): Attempt[DecodeResult[List[A]]] =
      cs.traverse(_.asDecoder).decode(bits)

    override def encode(value: List[A]): Attempt[BitVector] =
      if (cs.length == value.length) {
        cs.zip(value).foldMapM { case (c, v) => c.encode(v) }
      } else {
        Attempt.failure(Err("wrong length"))
      }
  }

  def sampleCodec(model: DataType, dCodec: Scalar => Codec[Data] = dataCodec): Codec[Sample] = {
    val (domainScalars: List[Scalar], rangeScalars: List[Scalar]) = model match {
      case s: Scalar =>
        (List[Scalar](), List[Scalar](s))
      case t: Tuple =>
        val tuples: List[Scalar] = t.flatElements.collect { case s: Scalar => s }
        (List[Scalar](), tuples)
      case Function(d, r) =>
        (d.getScalars, r.getScalars)
    }
    val domainList: List[Codec[Datum]] = domainScalars.map(s => dCodec(s).downcast[Datum])
    val rangeList: List[Codec[Data]] = rangeScalars.map(s => dCodec(s))
    codecOfList(domainList) ~ codecOfList(rangeList)
  }
}
