package latis.ops

import cats.effect.IO
import fs2.Pipe

import latis.data.Sample
import latis.model.DataType
import latis.util.LatisException

case class DropRight(n: Int) extends StreamOperation {

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = in => in.dropRight(n)

  def applyToModel(model: DataType): Either[LatisException, DataType] =
    Right(model)
}

object DropRight {
  def fromArgs(args: List[String]): Either[LatisException, DropRight] = args match {
    case n :: Nil => n.toIntOption.map(DropRight(_)).toRight(LatisException(s"Couldn't parse $n to int"))
    case _ => Left(LatisException("DropRight requires one argument"))
  }
}
