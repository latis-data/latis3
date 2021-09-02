package latis.ops

import cats.effect.IO
import cats.syntax.all._
import fs2.Pipe

import latis.data.Sample
import latis.model.DataType
import latis.util.LatisException

case class TakeRight(n: Int) extends StreamOperation with Taking {

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = in => in.takeRight(n)

  def applyToModel(model: DataType): Either[LatisException, DataType] =
    model.asRight
}

object TakeRight {
  def fromArgs(args: List[String]): Either[LatisException, TakeRight] = args match {
    case n :: Nil => Either.catchOnly[NumberFormatException](TakeRight(n.toInt))
      .leftMap(LatisException(_))
    case _ => Left(LatisException("TakeRight requires one argument"))
  }
}
