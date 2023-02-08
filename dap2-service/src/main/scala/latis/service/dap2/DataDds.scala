package latis.service.dap2

import java.nio.charset.StandardCharsets

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all._
import fs2.Stream

import latis.data._
import latis.dataset.Dataset
import latis.model._
import latis.service.dap2.AtomicTypeValue.ByteValue
import latis.service.dap2.DataDds.DdsValue
import latis.util.LatisException

final case class DataDds(dds: Dds, ddsValue: DdsValue, data: Stream[IO, AtomicTypeValue[_]]) {
  lazy val asBytes: EitherT[IO, LatisException, Stream[IO, Byte]] = {
    val ddsString = dds.asString
    val separator = "\r\nData:\r\n"
    val stringBinary = Stream.emits[IO, Byte]((ddsString + separator).getBytes(StandardCharsets.UTF_8))
    ddsValue.streamBuilder(data).flatMap(
      pair => EitherT(pair._2.compile.last.map {
        case Some(_) => Left(LatisException("There should not be any leftover AtomicTypeValues"))
        case None => Right(stringBinary ++ pair._1)
      })
    )
  }
}

/* TODO: Support DDS's Array and Grid type declarations */
object DataDds {
  val START_OF_INSTANCE: ByteValue = ByteValue(0x5A.toByte)
  val END_OF_SEQUENCE: ByteValue = ByteValue(0xA5.toByte)
  val START_BYTES: Array[Byte] = AtomicType.Byte.codec.encode(START_OF_INSTANCE).require.toByteArray
  val END_BYTES: Array[Byte] = AtomicType.Byte.codec.encode(END_OF_SEQUENCE).require.toByteArray

  sealed trait DdsValue {
    def streamBuilder: Stream[IO, AtomicTypeValue[_]] => EitherT[IO, LatisException, (Stream[IO, Byte], Stream[IO, AtomicTypeValue[_]])]
  }

  final case class AtomicValue[ATV<:AtomicTypeValue[_]](ty: AtomicType[ATV]) extends DdsValue {
    override def streamBuilder: Stream[IO, AtomicTypeValue[_]] => EitherT[IO, LatisException, (Stream[IO, Byte], Stream[IO, AtomicTypeValue[_]])] =
      (atvs: Stream[IO, AtomicTypeValue[_]]) => {
        val head = atvs.head.compile.last
        val tail = atvs.tail
        EitherT(head.map {
          case Some(av) => {
            try {
              val castAV = av.asInstanceOf[ATV]
              ty.codec.encode(castAV).fold(
                err => Left(LatisException(err.message)),
                bits => Right((Stream.emits[IO, Byte](bits.toByteArray), tail))
              )
            } catch {
              case _: Throwable => Left(LatisException(s"Atomic type $ty cannot be used to parse atomic type value $av"))
            }

          }
          case None => Left(LatisException("AtomicValue requires a nonempty stream of AtomicTypeValues"))
        })
      }
  }

  abstract sealed class ConstructorValue(fields: List[DdsValue]) extends DdsValue {
    override def streamBuilder: Stream[IO, AtomicTypeValue[_]] => EitherT[IO, LatisException, (Stream[IO, Byte], Stream[IO, AtomicTypeValue[_]])] =
      (atvs: Stream[IO, AtomicTypeValue[_]]) => {
        fields.foldLeft(
          EitherT.fromEither[IO](Right[LatisException, (Stream[IO, Byte], Stream[IO, AtomicTypeValue[_]])]((Stream.empty, atvs)).withLeft)
        ) { (either, ddsVal) =>
          either.flatMap { pair =>
            val builtStream = pair._1
            val remainder = pair._2
            ddsVal.streamBuilder(remainder).map { newPair =>
              val newStream = newPair._1
              val newRem = newPair._2
              (builtStream ++ newStream, newRem)
            }
          }
        }
      }
  }

  final case class StructureConstructor(fields: List[DdsValue]) extends ConstructorValue(fields)

  final case class SequenceConstructor(fields: List[DdsValue]) extends ConstructorValue(fields) {
    override def streamBuilder: Stream[IO, AtomicTypeValue[_]] => EitherT[IO, LatisException, (Stream[IO, Byte], Stream[IO, AtomicTypeValue[_]])] =
      (atvs: Stream[IO, AtomicTypeValue[_]]) => {
        val ioLst = atvs.compile.toList
        val ioOutput: IO[Either[LatisException, (Stream[IO, Byte], Stream[IO, AtomicTypeValue[_]])]] = ioLst.flatMap { lst =>
          if (lst.head != START_OF_INSTANCE) {
            if (lst.head != END_OF_SEQUENCE)
              IO(Left(LatisException("Sequence must start with a valid marker")))
            else
              IO(Right((Stream.emits(END_BYTES), atvs.tail)))
          } else {
            val findEnd = lst.tail.foldLeft((1,1,false)) { (acc, elem) =>
              val needsClose = acc._1
              val index = acc._2
              val flag = acc._3
              if (!flag) {
                if (elem == START_OF_INSTANCE)
                  (needsClose + 1, index + 1, false)
                else if (elem == END_OF_SEQUENCE) {
                  val newClose = needsClose - 1
                  val newFlag = newClose == 0
                  val newIdx = if (!newFlag) index + 1 else index
                  (newClose, newIdx, newFlag)
                }
                else
                  (needsClose, index + 1, false)
              }
              else
                (needsClose, index, true)
            }
            if (!findEnd._3)
              IO(Left(LatisException("Sequence must be terminated by an unpaired END_OF_SEQUENCE marker")))
            else {
              val end = findEnd._2
              val remainderForSeq = lst.slice(1, end)
              val remainderPassAlong = atvs.takeRight(lst.length - end - 1)
              val folded = remainderForSeq.foldLeft(
                IO((Stream[IO, Byte](), Stream[IO, AtomicTypeValue[_]]()))
              ) { (ioPair, currAtv) =>
                ioPair.flatMap { pair =>
                  val builtStream = pair._1
                  val currRem = pair._2
                  val newRem = currRem ++ Stream.emit(currAtv)
                  super.streamBuilder(newRem).fold(
                    _ => (builtStream, newRem),
                    newPair => {
                      val newStream = newPair._1
                      val newRem = newPair._2
                      (builtStream ++ newStream, newRem)
                    }
                  )
                }
              }
              EitherT(folded.flatMap { p =>
                val newStream = p._1
                val newRem = p._2
                newRem.compile.last.map {
                  case Some(x) => Left(LatisException("Sequence must use all values between the markers"))
                  case None => Right(
                    (Stream.emits(START_BYTES) ++ newStream ++ Stream.emits(END_BYTES), remainderPassAlong)
                  )
                }
              }).value
            }
          }
        }
        EitherT(ioOutput)
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
      Stream.eval(ioStream).flatten
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