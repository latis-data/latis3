package latis.service.dap2

import java.nio.charset.StandardCharsets

import cats.effect.IO
import cats.syntax.all._
import fs2.Stream
import scodec.Codec
import scodec.codecs
import scodec.stream.StreamEncoder

import latis.data.Sample
import latis.dataset.Dataset
import latis.model._
import latis.output.BinaryEncoder
import latis.service.dap2.AtomicType.zeroByte
import latis.service.dap2.DataDds.DdsValue
import latis.util.LatisException

final case class DataDds(dds: Dds, ddsValue: DdsValue, data: Stream[IO, _]) {
  val encoder: StreamEncoder[_] = StreamEncoder.many(ddsValue.codec)

  def asBytes(): Stream[IO, Byte] = {
    val ddsString = dds.asString
    val separator = "\r\nData:\r\n"
    val stringBinary = (ddsString + separator).getBytes(StandardCharsets.UTF_8)
    val dataBinary = data.through(encoder.toPipeByte[IO])
    Stream.emits(stringBinary) ++ dataBinary
  }
}

/* TODO: Support DDS's Array and Grid type declarations */
object DataDds {
  sealed trait DdsValue {
    val codec: Codec[_]
  }
  final case class AtomicValue[F](ty: AtomicType[F]) extends DdsValue {
    override val codec = ty.codec
  }
  sealed trait ConstructorValue extends DdsValue

  final case class StructureConstructor(fields: List[DdsValue]) extends ConstructorValue {
    override val codec = BinaryEncoder.codecOfList(fields.map(_.codec))
  }
  final case class SequenceConstructor(fields: List[DdsValue]) extends ConstructorValue {
    val START_OF_INSTANCE = codecs.byte.unit(0x5A.toByte) ~ zeroByte ~ zeroByte ~ zeroByte
    val END_OF_SEQUENCE = codecs.byte.unit(0xA5.toByte) ~ zeroByte ~ zeroByte ~ zeroByte

    override val codec = {
      if (fields.nonEmpty) {
        START_OF_INSTANCE ~ BinaryEncoder.codecOfList(fields.map(_.codec)) ~ END_OF_SEQUENCE
      }
      else {
        END_OF_SEQUENCE
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

  def fromDataset(dataset: Dataset): Either[LatisException, DataDds] = {
    for {
      dds <- Dds.fromDataset(dataset)
      ddsValues <- fromDataType(dataset.model)
      data = dataset.samples
    } yield {
      DataDds(dds, ddsValues, data)
    }
  }
}