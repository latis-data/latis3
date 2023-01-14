package latis.service.dap2

import java.nio.charset.StandardCharsets

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import fs2.Stream
import scodec._
import scodec.bits.BitVector

import latis.data.Data
import latis.data.Sample
import latis.data._
import latis.dataset.Dataset
import latis.model._
import latis.output.BinaryEncoder.codecOfList
import latis.service.dap2.AtomicTypeValue.ByteValue
import latis.service.dap2.DataDds.DdsValue
import latis.util.LatisException

final case class DataDds(dds: Dds, ddsValue: DdsValue, data: Stream[IO, AtomicTypeValue[_]]) {

  def asBytes(): Stream[IO, Byte] = {
    val ddsString = dds.asString
    val separator = "\r\nData:\r\n"
    val stringBinary = (ddsString + separator).getBytes(StandardCharsets.UTF_8)
    val dataBinary = ddsValue.codec.encode(data.compile.toList.unsafeRunSync()).require.toByteArray /** Yuck... */
    Stream.emits(stringBinary) ++ Stream.emits(dataBinary)
  }
}

/* TODO: Support DDS's Array and Grid type declarations */
object DataDds {
  sealed trait DdsValue {
    val codecs: List[Codec[AtomicTypeValue[_]]] = List()
    val codec: Codec[List[AtomicTypeValue[_]]] = codecOfList(codecs)
  }
  final case class AtomicValue(ty: AtomicType[AtomicTypeValue[_]]) extends DdsValue {
    override val codecs: List[Codec[AtomicTypeValue[_]]] = List(ty.codec)
  }
  sealed class ConstructorValue(fields: List[DdsValue]) extends DdsValue {
    override val codecs: List[Codec[AtomicTypeValue[_]]] = fields.flatMap(_.codecs)
    lazy val flatten: List[AtomicValue] = fields.foldLeft(List.empty[AtomicValue]) { (lst, f) =>
      val newLst = f match {
        case av: AtomicValue => List(av)
        case cv: ConstructorValue => cv.flatten
      }
      lst ++ newLst
    }
  }

  final case class StructureConstructor(fields: List[DdsValue]) extends ConstructorValue(fields)

  final case class SequenceConstructor(fields: List[DdsValue]) extends ConstructorValue(fields) {
    val START_OF_INSTANCE: Array[Byte] = AtomicType.Byte.codec.encode(ByteValue(0x5A.toByte)).require.toByteArray
    val END_OF_SEQUENCE: Array[Byte] = AtomicType.Byte.codec.encode(ByteValue(0xA5.toByte)).require.toByteArray

    override val codec: Codec[List[AtomicTypeValue[_]]] = {
      val elemCodec = codecOfList(codecs)
      new Codec[List[List[AtomicTypeValue[_]]]] {
        override def decode(bits: BitVector): Attempt[DecodeResult[List[List[AtomicTypeValue[_]]]]] = {
          val bytes = bits.toByteArray
          if (bytes.startsWith(END_OF_SEQUENCE)) {
            Attempt.successful(DecodeResult(
              List(),
              BitVector(bytes.slice(END_OF_SEQUENCE.length, bytes.length))
            ))
          }
          else if (bytes.startsWith(START_OF_INSTANCE)) {
            val end = bytes.lastIndexOfSlice(END_OF_SEQUENCE)
            if (end >= 0) {
              val result = bytes.grouped(4).foldLeft((List.empty[List[AtomicTypeValue[_]]], BitVector.empty)) { (acc, byteGroup) =>
                val decoded = acc._1
                val queue = acc._2 ++ BitVector(byteGroup)
                elemCodec.decode(queue).fold(
                  _ => (decoded, queue),
                  lst => (decoded :+ lst.value, BitVector.empty)
                )
              }
              Attempt.successful(DecodeResult(result._1, result._2))
            }
            else {
              Attempt.failure(Err("Sequence did not end with a valid suffix"))
            }
          }
          else {
            Attempt.failure(Err("Sequence did not start with a valid prefix"))
          }
        }

        override def encode(value: List[List[AtomicTypeValue[_]]]): Attempt[BitVector] = {
          if (value.nonEmpty) {
            value.foldLeft(Attempt.successful(BitVector.empty)) { (combo, lst) =>
              for {
                c <- combo
                l <- elemCodec.encode(lst)
              } yield {
                c ++ l
              }
            }.map(BitVector(START_OF_INSTANCE) ++ _ ++ BitVector(END_OF_SEQUENCE))
          } else {
            Attempt.successful(BitVector(END_OF_SEQUENCE))
          }
        }

        override def sizeBound: SizeBound = SizeBound(0, None)
      }
    }.xmap[List[AtomicTypeValue[_]]](
      sqLst => sqLst.flatten,
      ftLst => List(ftLst) /**DANGER WRONG*/
    )
  }

  private def fromScalar[F<:AtomicTypeValue[T], T](scalar: Scalar): Either[LatisException, AtomicValue] =
    AtomicType.fromScalar(scalar).map(at => AtomicValue(at.asInstanceOf[AtomicType[AtomicTypeValue[_]]]))

  private def fromTuple(tuple: Tuple): Either[LatisException, StructureConstructor] =
    tuple.elements.traverse(fromDataType).map(StructureConstructor)

  private def fromFunction(func: Function): Either[LatisException, SequenceConstructor] =
    List(fromDataType(func.domain), fromDataType(func.range)).sequence.map(SequenceConstructor)

  private[dap2] def fromDataType(dt: DataType): Either[LatisException, DdsValue] = dt match {
    case s: Scalar => fromScalar(s)
    case t: Tuple => fromTuple(t)
    case f: Function => fromFunction(f)
  }

  private def convertData(data: Data): Stream[IO, AtomicTypeValue[_]] = data match {
    case NullData => Stream.empty
    case datum: Datum => Stream.fromEither[IO](AtomicTypeValue.fromDatum(datum))
    case tuple: TupleData => Stream.emits(tuple.elements.map(convertData)).flatten
    case func: SampledFunction => streamSamples(func.samples)
  }
  private[dap2] def streamSamples(samples: Stream[IO, Sample]): Stream[IO, AtomicTypeValue[_]] =
    samples.flatMap { s =>
      val domain = s._1
      val range = s._2
      val converted = (domain ++ range).map(convertData)
      converted.foldLeft(Stream[IO,AtomicTypeValue[_]]())((acc, str) => acc.append(str))
    }

  def fromDataset(dataset: Dataset): Either[LatisException, DataDds] = {
    for {
      dds <- Dds.fromDataset(dataset)
      ddsValue <- fromDataType(dataset.model)
      data = streamSamples(dataset.samples)
    } yield {
      DataDds(dds, ddsValue, data)
    }
  }
}