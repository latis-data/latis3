package latis.ops

import cats.effect.IO
import cats.syntax.all._
import fs2.Pipe

import latis.data.Sample
import latis.model.DataType
import latis.util.LatisException

class Take private (val n: Int) extends StreamOperation with Taking {

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = in => in.take(n.toLong)

  def applyToModel(model: DataType): Either[LatisException, DataType] =
    model.asRight
}

object Take {

  /**
   * Constructs a Take operation.
   * A negative number will be treated as zero.
   */
  def apply(n: Int): Take = {
    if (n < 0) new Take(0)
    else new Take(n)
  }

  def fromArgs(args: List[String]): Either[LatisException, Take] = args match {
    case n :: Nil => Either.catchOnly[NumberFormatException](Take(n.toInt))
      .leftMap(LatisException(_))
    case _ => Left(LatisException("Take requires one argument"))
  }

  def unapply(t: Take): Option[Int] = Option(t.n)
}
