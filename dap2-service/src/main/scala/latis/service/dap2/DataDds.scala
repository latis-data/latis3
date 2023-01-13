package latis.service.dap2

import java.nio.charset.StandardCharsets

import cats.effect.IO
import cats.syntax.all._
import fs2.Stream
import scodec.Attempt
import scodec.Codec
import scodec.DecodeResult
import scodec.Err
import scodec.SizeBound
import scodec.bits.BitVector
import scodec.stream.StreamEncoder

import latis.data.Data
import latis.data.Datum
import latis.data.NullData
import latis.data.Sample
import latis.data.SampledFunction
import latis.data.TupleData
import latis.dataset.Dataset
import latis.model._
import latis.output.BinaryEncoder
import latis.output.BinaryEncoder.codecOfList
import latis.service.dap2.DataDds.DdsValue
import latis.util.LatisException

final case class DataDds(dds: Dds, ddsValue: DdsValue, data: Stream[IO, Array[Byte]]) {

  def asBytes(): Stream[IO, Byte] = {
    val ddsString = dds.asString
    val separator = "\r\nData:\r\n"
    val stringBinary = (ddsString + separator).getBytes(StandardCharsets.UTF_8)
    val dataBinary = data.through(ddsValue.encoder.toPipeByte[IO])
    Stream.emits(stringBinary) ++ dataBinary
  }
}

/* TODO: Support DDS's Array and Grid type declarations */
object DataDds {
  sealed trait DdsValue {
    val codec: Codec[_]
  }
  final case class AtomicValue[F](ty: AtomicType[F]) extends DdsValue {
    override val codec: Codec[F] = ty.codec
  }
  sealed trait ConstructorValue extends DdsValue

  final case class StructureConstructor(fields: List[DdsValue]) extends ConstructorValue {
    override val codec: Codec[List[_]] = codecOfList(fields.map(_.codec))
  }
  final case class SequenceConstructor(fields: List[DdsValue]) extends ConstructorValue {
    val START_OF_INSTANCE: Array[Byte] = AtomicType.Byte.padBytes(Array(0x5a.toByte))
    val END_OF_SEQUENCE: Array[Byte] = AtomicType.Byte.padBytes(Array(0xA5.toByte))

    override val codec: Codec[List[List[_]]] = {
      val elemCodec = codecOfList(fields.map(_.codec))
      new Codec[List[List[_]]] {
        override def decode(bits: BitVector): Attempt[DecodeResult[List[List[_]]]] = {
          val bytes = bits.toByteArray
          if (bytes.startsWith(END_OF_SEQUENCE)) {
            Attempt.successful(DecodeResult(
              List(List()),
              BitVector(bytes.slice(END_OF_SEQUENCE.length, bytes.length))
            ))
          }
          else if (bytes.startsWith(START_OF_INSTANCE)) {
            val end = bytes.lastIndexOfSlice(END_OF_SEQUENCE)
            if (end >= 0) {
              val toDecode = bytes.slice(START_OF_INSTANCE.length, end)
              val remain = bytes.slice(end + END_OF_SEQUENCE.length, bytes.length)
              val res = toDecode.foldLeft((Attempt.successful(List(List.empty[_])), Array[Byte]())) { (acc, byte) =>
                val decoded: Attempt[List[List[_]]] = acc._1
                val queue: Array[Byte] = acc._2 ++ byte
                val newChunk = elemCodec.decode(BitVector(queue))
                newChunk.fold(
                  err => (decoded, queue),
                  value => (decoded.map(_ :+ value.value), value.remainder.toByteArray)
                )
              }
              if (res._1.isFailure || res._2.nonEmpty) {
                Attempt.failure(Err("Sequence could not be decoded properly"))
              }
              else {
                Attempt.successful(DecodeResult(
                  res._1.require,
                  BitVector(remain)
                ))
              }
            }
            else {
              Attempt.failure(Err("Sequence did not end with a valid suffix"))
            }
          }
          else {
            Attempt.failure(Err("Sequence did not start with a valid prefix"))
          }
        }

        override def encode(value: List[List[_]]): Attempt[BitVector] = {
          if (value.nonEmpty) {
            value.foldLeft(Attempt.successful(BitVector.empty)) { (combo, lst) =>
              for {
                c <- combo
                l <- elemCodec.encode(lst)
              } yield {
                c ++ l
              }
            }.map(BitVector(START_OF_INSTANCE) ++ _ ++ BitVector(END_OF_SEQUENCE))
          }
          else Attempt.successful(BitVector(END_OF_SEQUENCE))
        }

        override def sizeBound: SizeBound = SizeBound(0, None)
      }
    }
  }

  private def fromScalar(scalar: Scalar): Either[LatisException, AtomicValue[_]] =
    AtomicType.fromScalar(scalar).map(AtomicValue(_))

  private def fromTuple(tuple: Tuple): Either[LatisException, StructureConstructor] =
    tuple.elements.traverse(fromDataType).map(StructureConstructor)

  private def fromFunction(func: Function): Either[LatisException, SequenceConstructor] =
    List(fromDataType(func.domain), fromDataType(func.range)).sequence.map(SequenceConstructor)

  private[dap2] def fromDataType(dt: DataType): Either[LatisException, DdsValue] = dt match {
    case s: Scalar => fromScalar(s)
    case t: Tuple => fromTuple(t)
    case f: Function => fromFunction(f)
  }

  private[dap2] def arrayFromData(data: Data): Stream[IO, Array[Byte]] = data match {
    case NullData => Stream.empty
    case datum: Datum => Stream.fromEither(AtomicType.encodeDatum(datum))
    case tuple: TupleData => Stream.emits(tuple.elements.map(arrayFromData)).flatten
    case func: SampledFunction => arrayFromSamples(func.samples)
  }

  private[dap2] def arrayFromSamples(samples: Stream[IO, Sample]): Stream[IO, Array[Byte]] =
    samples.map(s => s._1.traverse(arrayFromData) ++ s._2.traverse(arrayFromData))
      .flatten.map(s => s.flatten.toArray)

  def fromDataset(dataset: Dataset): Either[LatisException, DataDds] = {
    for {
      dds <- Dds.fromDataset(dataset)
      ddsValues <- fromDataType(dataset.model)
      data = ddsValues.
    } yield {
      DataDds(dds, ddsValues, data)
    }
  }
}