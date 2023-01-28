package latis.service.dap2

import java.nio.charset.StandardCharsets

import scala.collection.immutable.Queue

import cats.Id
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import fs2.Pure
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
    val dataBinary = ddsValue.streamBuilder(data.compile.toList.unsafeRunSync()) /* TODO: Remove unsafeRun */
    Stream.emits(stringBinary) ++ Stream.fromEither[IO](dataBinary).flatMap(_._1)
  }
}

/* TODO: Support DDS's Array and Grid type declarations */
object DataDds {
  val START_OF_INSTANCE: ByteValue = ByteValue(0x5A.toByte)
  val END_OF_SEQUENCE: ByteValue = ByteValue(0xA5.toByte)

  sealed trait DdsValue {
    /* TODO: Use a Stream[IO, AtomicTypeValue[_]] as the input instead of List[AtomicTypeValue[_]] */
    val streamBuilder: List[AtomicTypeValue[_]] => Either[LatisException, (Stream[IO, Byte], List[AtomicTypeValue[_]])]
  }

  final case class AtomicValue(ty: AtomicType[AtomicTypeValue[_]]) extends DdsValue {
    override val streamBuilder: List[AtomicTypeValue[_]] => Either[LatisException, (Stream[IO, Byte], List[AtomicTypeValue[_]])] =
      (atvs: List[AtomicTypeValue[_]]) => {
        val head = atvs.head
        val tail = atvs.tail
        ty.codec.encode(head).fold(
          err => Left(LatisException(err.message)),
          bits => Right((Stream.emits(bits.toByteArray), tail))
        )
      }
  }

  abstract sealed class ConstructorValue(fields: List[DdsValue]) extends DdsValue {
    override val streamBuilder: List[AtomicTypeValue[_]] => Either[LatisException, (Stream[IO, Byte], List[AtomicTypeValue[_]])] =
      (atvs: List[AtomicTypeValue[_]]) => {
        fields.foldLeft(
          Right[LatisException, (Stream[IO, Byte], List[AtomicTypeValue[_]])](Stream.empty, atvs).withLeft
        ) { (either, ddsVal) =>
          either.fold(err => Left(err), { pair =>
            val builtStream = pair._1
            val remainder = pair._2
            ddsVal.streamBuilder(remainder).map { newPair =>
              val newStream = newPair._1
              val newRem = newPair._2
              (builtStream ++ newStream, newRem)
            }
          })
        }
      }
  }

  final case class StructureConstructor(fields: List[DdsValue]) extends ConstructorValue(fields)

  final case class SequenceConstructor(fields: List[DdsValue]) extends ConstructorValue(fields) {
    override val streamBuilder: List[AtomicTypeValue[_]] => Either[LatisException, (Stream[IO, Byte], List[AtomicTypeValue[_]])] =
      (atvs: List[AtomicTypeValue[_]]) => {
        if (atvs.head != START_OF_INSTANCE)
          Left(LatisException("Sequence must start with a START_OF_INSTANCE marker"))
        else {
          val end = atvs.indexOf(END_OF_SEQUENCE)
          if (end == -1)
            Left(LatisException("Sequence must have an END_OF_SEQUENCE marker"))
          else {
            val remainderForSeq = atvs.slice(1, end)
            val remainderPassAlong = atvs.slice(end+1, atvs.length)
            val p = remainderForSeq.foldLeft(
              (Stream[IO, Byte](), List.empty[AtomicTypeValue[_]])
            ) { (pair, currAtv) =>
              val builtStream = pair._1
              val currRem = pair._2
              val newRem = currRem :+ currAtv
              super.streamBuilder(newRem).fold(
                err => (builtStream, newRem),
                newPair => {
                  val newStream: Stream[IO, Byte] = newPair._1
                  val newRem = newPair._2
                  (builtStream ++ newStream, newRem)
                }
              )
            }
            val newStream = p._1
            val newRem = p._2
            if (newRem.isEmpty)
              Right(newStream, remainderPassAlong)
            else
              Left(LatisException("Sequence must use all bytes between the markers"))
          }
        }
      }
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
    case tuple: TupleData => streamSamples(tuple.samples)
    case func: SampledFunction => streamSamples(func.samples)
  }
  private[dap2] def streamSamples(samples: Stream[IO, Sample]): Stream[IO, AtomicTypeValue[_]] =
    samples.flatMap { s =>
      val domain = s._1
      val range = s._2
      val converted = (domain ++ range).map(convertData)
        .foldLeft(Stream[IO,AtomicTypeValue[_]]())((acc, str) => acc.append(str))
      val ioStream = converted.head.compile.toList.map { lst =>
        if (lst.isEmpty)
          Stream.emit(END_OF_SEQUENCE)
        else
          Stream.emit(START_OF_INSTANCE) ++ converted ++ Stream.emit(END_OF_SEQUENCE)
      }
      Stream.evalSeq(ioStream)
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