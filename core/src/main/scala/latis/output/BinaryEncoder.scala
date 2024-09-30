package latis.output

import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream
import scodec.bits.*
import scodec.interop.cats.*
import scodec.stream.StreamEncoder
import scodec.{Encoder as _, *}

import latis.data.*
import latis.dataset.*
import latis.model.*
import latis.ops.Uncurry

class BinaryEncoder(val dataCodec: Scalar => Codec[Data] = DataCodec.defaultDataCodec) extends Encoder[IO, Byte] {
  //TODO: deal with NullData, require replaceMissing?

  /**
   * Encodes the Stream of Samples from the given Dataset as a Stream
   * of BitVectors.
   */
  override def encode(dataset: Dataset): Stream[IO, Byte] = {
    val uncurriedDataset = dataset.withOperation(Uncurry())
    // Encode the samples as a Stream of Bytes
    uncurriedDataset.samples.through(sampleStreamEncoder(uncurriedDataset.model).toPipeByte)
  }

  /** Instance of scodec.stream.StreamEncoder for Sample. */
  def sampleStreamEncoder(model: DataType): StreamEncoder[Sample] =
    StreamEncoder.many(sampleCodec(model))

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

  def sampleCodec(model: DataType): Codec[Sample] = {
    val (domainScalars: List[Scalar], rangeScalars: List[Scalar]) = model match {
      case s: Scalar =>
        (List[Scalar](), List[Scalar](s))
      case t: Tuple =>
        val tuples: List[Scalar] = t.flatElements.collect { case s: Scalar => s }
        (List[Scalar](), tuples)
      case Function(d, r) =>
        (d.getScalars.filterNot(_.isInstanceOf[Index]), r.getScalars)
    }
    val domainList: List[Codec[Datum]] = domainScalars.map(s => dataCodec(s).downcast[Datum])
    val rangeList: List[Codec[Data]] = rangeScalars.map(s => dataCodec(s))
    // TODO: Could use .as[Sample] here, but it can't tell they are
    // isomorphic for reasons I don't understand.
    (codecOfList(domainList) :: codecOfList(rangeList)).xmap(
      (d, r) => Sample(d, r),
      s => (s.domain, s.range)
    )
  }
}

object BinaryEncoder {
  def apply(dataCodec: Scalar => Codec[Data] = DataCodec.defaultDataCodec): BinaryEncoder = new BinaryEncoder(dataCodec)
}
