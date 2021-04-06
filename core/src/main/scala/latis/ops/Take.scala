package latis.ops

import cats.effect.IO
import cats.syntax.all._
import fs2.Pipe

import latis.data.Sample
import latis.model.DataType
import latis.util.LatisException

case class Take(n: Long) extends StreamOperation {
  def pipe(model: DataType): Pipe[IO, Sample, Sample] = in => in.take(n)

  def applyToModel(model: DataType): Either[LatisException, DataType] =
    model.asRight
}

object Take {
  def fromArgs(args: List[String]): Either[LatisException, Take] = args match {
    case n :: Nil =>
      Either
        .catchOnly[NumberFormatException](Take(n.toLong))
        .leftMap(LatisException(_))
    case _ => Left(LatisException("Take requires one argument"))
  }
}
