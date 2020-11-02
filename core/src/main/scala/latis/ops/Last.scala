package latis.ops

import cats.effect.IO
import fs2.Pipe

import latis.data.Sample
import latis.model.DataType
import latis.util.LatisException

case class Last() extends StreamOperation {

  def pipe(model: DataType): Pipe[IO, Sample, Sample] =
    in => in.lastOr(Sample(Seq.empty, Seq.empty))

  def applyToModel(model: DataType): Either[LatisException, DataType] =
    Right(model)
}
