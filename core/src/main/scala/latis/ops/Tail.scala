package latis.ops

import cats.effect.IO
import cats.syntax.all.*
import fs2.Pipe

import latis.data.Sample
import latis.model.DataType
import latis.util.LatisException

case class Tail() extends StreamOperation {

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = in => in.tail

  def applyToModel(model: DataType): Either[LatisException, DataType] =
    model.asRight
}

object Tail {

  def builder: OperationBuilder = (args: List[String]) => {
    if (args.nonEmpty) LatisException("Tail does not take arguments").asLeft
    else Tail().asRight
  }
}
