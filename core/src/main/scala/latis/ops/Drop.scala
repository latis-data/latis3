package latis.ops

import cats.effect.IO
import cats.syntax.all._
import fs2.Pipe

import latis.data.Sample
import latis.model.DataType
import latis.util.LatisException

case class Drop(n: Long) extends StreamOperation {

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = in => in.drop(n)

  def applyToModel(model: DataType): Either[LatisException, DataType] =
    model.asRight
}

object Drop {

  def builder: OperationBuilder = (args: List[String]) => fromArgs(args)

  def fromArgs(args: List[String]): Either[LatisException, Drop] = args match {
    case n :: Nil => Either.catchOnly[NumberFormatException](Drop(n.toLong))
      .leftMap(LatisException(_))
    case _ => Left(LatisException("Drop requires one argument"))
  }
}
