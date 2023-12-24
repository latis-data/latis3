package latis.ops

import cats.effect.IO
import cats.syntax.all.*
import fs2.Pipe

import latis.data.Sample
import latis.model.DataType
import latis.util.LatisException

case class DropLast() extends StreamOperation {

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = in => in.dropLast

  def applyToModel(model: DataType): Either[LatisException, DataType] =
    model.asRight
}

object DropLast {

  def builder: OperationBuilder = (args: List[String]) => {
    if (args.nonEmpty) LatisException("DropLast does not take arguments").asLeft
    else DropLast().asRight
  }
}
