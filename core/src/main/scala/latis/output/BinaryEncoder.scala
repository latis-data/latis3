package latis.output

import cats.effect.IO
import cats.syntax.all._
import fs2.Stream
import scodec.Codec
import scodec.bits._
import scodec.interop.cats._
import scodec.stream.StreamEncoder
import scodec.{Encoder => _, _}

import latis.data._
import latis.dataset._
import latis.model._
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
        (d.getScalars, r.getScalars)
    }
    val domainList: List[Codec[Datum]] = domainScalars.map(s => dataCodec(s).downcast[Datum])
    val rangeList: List[Codec[Data]] = rangeScalars.map(s => dataCodec(s))
    codecOfList(domainList) ~ codecOfList(rangeList)
  }
}
