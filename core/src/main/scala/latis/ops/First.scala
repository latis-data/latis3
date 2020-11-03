package latis.ops
import cats.effect.IO
import cats.implicits._
import fs2.Pipe

import latis.data.Sample
import latis.model.DataType
import latis.util.LatisException

case class First() extends StreamOperation {

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = in => in.head

  def applyToModel(model: DataType): Either[LatisException, DataType] =
    model.asRight
}
