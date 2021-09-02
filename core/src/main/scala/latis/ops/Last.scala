package latis.ops

import cats.effect.IO
import cats.syntax.all._
import fs2.Pipe
import fs2.Stream

import latis.data.Sample
import latis.model.DataType
import latis.util.LatisException

case class Last() extends StreamOperation with Taking {

  def pipe(model: DataType): Pipe[IO, Sample, Sample] =
    in =>
      in.last.flatMap {
        case Some(s: Sample) => Stream(s)
        case None            => Stream.empty
      }

  def applyToModel(model: DataType): Either[LatisException, DataType] =
    model.asRight
}
