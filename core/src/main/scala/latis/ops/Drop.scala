package latis.ops

import cats.effect.IO
import fs2.Pipe

import latis.data.Sample
import latis.model.DataType
import latis.util.LatisException

case class Drop(n: Long) extends StreamOperation {

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = in => in.drop(n)

  def applyToModel(model: DataType): Either[LatisException, DataType] =
    Right(model)
}

object Drop {
  def fromArgs(args: List[String]): Either[LatisException, Drop] = args match {
    case n :: Nil => n.toLongOption.map(Drop(_)).toRight(LatisException(s"Couldn't parse $n to long"))
    case _ => Left(LatisException("Drop requires one argument"))
  }
}
