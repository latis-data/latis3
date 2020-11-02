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
