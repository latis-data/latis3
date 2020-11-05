package latis.ops

import cats.effect.IO
import fs2.Pipe

import latis.data.Sample
import latis.model.DataType
import latis.util.LatisException

case class TakeRight(n: Int) extends StreamOperation {

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = in => in.takeRight(n)

  def applyToModel(model: DataType): Either[LatisException, DataType] =
    Right(model)
}

object TakeRight {
  def fromArgs(args: List[String]): Either[LatisException, TakeRight] = args match {
    case n :: Nil => n.toIntOption.map(TakeRight(_)).toRight(LatisException(s"Couldn't parse $n to int"))
    case _ => Left(LatisException("TakeRight requires one argument"))
  }
}
