package latis.ops

import cats.effect.IO
import fs2.Pipe

import latis.data.Sample
import latis.model.DataType
import latis.util.LatisException

case class Take(n: Long) extends StreamOperation {

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = in => in.take(n)

  def applyToModel(model: DataType): Either[LatisException, DataType] =
    Right(model)
}

object Take {
  def fromArgs(args: List[String]): Either[LatisException, Take] = args match {
    case n :: Nil => n.toLongOption.map(Take(_)).toRight(LatisException(s"Couldn't parse $n to long"))
    case _ => Left(LatisException("Take requires one argument"))
  }
}
