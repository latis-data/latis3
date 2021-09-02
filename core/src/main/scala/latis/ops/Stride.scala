package latis.ops

import cats.effect.IO
import fs2._

import latis.data.Sample
import latis.model.DataType
import latis.util.LatisException

/**
 * Defines a unary operation that keeps every nth Sample
 * where n is the stride. The stride can have more than one element,
 * one for each dimension of a Cartesian Dataset.
 */
case class Stride(stride: Seq[Int]) extends StreamOperation {
  //TODO: support nD stride for Cartesian Datasets

  def pipe(model: DataType): Pipe[IO, Sample, Sample] =
    (stream: Stream[IO, Sample]) => stream.filter(predicate)

  private val predicate: Sample => Boolean = {
    var count = -1
    (_: Sample) => {
      count = count + 1
      if (count % stride.head == 0) true
      else false
    }
  }

  // A Stride does not affect the model.
  def applyToModel(model: DataType): Either[LatisException, DataType] = Right(model)

}

object Stride {
  def apply(n: Int): Stride = Stride(Seq(n))
}
