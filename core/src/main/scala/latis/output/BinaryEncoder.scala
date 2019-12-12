package latis.output

import cats.effect.IO
import fs2.Stream
import scodec._
import scodec.Codec
import scodec.bits._
import scodec.codecs.implicits._
import scodec.stream.{encode => sEncode,StreamEncoder}
import scodec.{Encoder => SEncoder}
import latis.data.Data._
import latis.data._
import latis.dataset._
import latis.ops.Uncurry

class BinaryEncoder extends Encoder[IO, BitVector] {

  /**
   * Encodes the Stream of Samples from the given Dataset as a Stream
   * of BitVectors.
   */
  override def encode(dataset: Dataset): Stream[IO, BitVector] = {
    val uncurriedDataset = dataset.withOperation(Uncurry())
    // Encode each Sample as a BitVector in the Stream
    sampleStreamEncoder.encode(uncurriedDataset.samples)
  }

  /** Instance of scodec.stream.StreamEncoder for Sample. */
  implicit val sampleStreamEncoder: StreamEncoder[Sample] =
    sEncode.many(sampleEncoder)

  /** Instance of scodec.Encoder for Sample. */
  implicit val sampleEncoder: SEncoder[Sample] = new SEncoder[(DomainData, RangeData)] {
    override def encode(value: (DomainData, RangeData)): Attempt[BitVector] = value match {
      case Sample(ds, rs) =>
        (ds ++ rs).foldLeft(Attempt.successful(BitVector(hex""))) {
          case (ab: Attempt[BitVector], d: Data) =>
            ab.flatMap {
              case b: BitVector => SEncoder.encode(d).map(b ++ _)
            }
        }
    }
    override def sizeBound: SizeBound = new SizeBound(0, None)
  }

  /** Instance of scodec.Encoder for Data. */
  implicit val encodeData: SEncoder[Data] = new SEncoder[Data] {
    override def encode(value: Data): Attempt[BitVector] = value match {
      case x: BooleanValue    => Codec.encode(x.value)
      case x: ByteValue       => Codec.encode(x.value)
//      case x: CharValue       => Codec.encode(x.value)
      case x: ShortValue      => Codec.encode(x.value)
      case x: IntValue        => Codec.encode(x.value)
      case x: LongValue       => Codec.encode(x.value)
      case x: FloatValue      => Codec.encode(x.value)
      case x: DoubleValue     => Codec.encode(x.value)
//      case x: BinaryValue     => Codec.encode(x.value)
      case x: StringValue     => Codec.encode(x.value)
//      case x: BigIntValue     => Codec.encode(x.value)
//      case x: BigDecimalValue => Codec.encode(x.value)
      case _                  => Attempt.failure(Err("No encoder for given datatype."))
    }
    override def sizeBound: SizeBound = new SizeBound(0, None)
  }
}
