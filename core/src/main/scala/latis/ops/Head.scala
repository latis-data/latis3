package latis.ops

import cats.effect.IO
import cats.syntax.all._
import fs2.Pipe

import latis.data.Sample
import latis.model.DataType
import latis.util.LatisException

case class Head() extends StreamOperation with Taking {

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = in => in.head

  def applyToModel(model: DataType): Either[LatisException, DataType] =
    model.asRight
}

object Head {

  def builder: OperationBuilder = (args: List[String]) => {
    if (args.nonEmpty) LatisException("Head does not take arguments").asLeft
    else Head().asRight
  }
}
