package latis.ops

import cats.effect.IO
import cats.syntax.all.*
import fs2.Pipe
import fs2.Stream

import latis.data.*
import latis.model.*
import latis.util.LatisException

/**
 * Defines an Operation that maps a function of Sample => Sample
 * over the data of a Dataset to generate a new Dataset
 * with each Sample modified. The resulting Dataset should
 * have the same number of Samples.
 */
trait MapOperation extends StreamOperation { self =>

  /**
   * Defines a function that modifies a given Sample
   * into a new Sample.
   */
  def mapFunction(model: DataType): Sample => Sample

  /**
   * Implements a Pipe in terms of the mapFunction.
   * Drop any Sample that results in an exception.
   */
  def pipe(model: DataType): Pipe[IO, Sample, Sample] = {
    val f = mapFunction(model)
    (stream: Stream[IO, Sample]) =>
      stream.map { sample =>
        Either.catchNonFatal(f(sample))
      }.evalTapChunk {
        case Left(t) =>
          val msg = s"[WARN] Sample dropped. $t"
          IO.println(msg) //TODO: log
        case _ => IO.unit
      }.collect {
        case Right(s) => s
      }
  }

  /**
   * Composes this operation with the given MapOperation.
   * Note that the given Operation will be applied first.
   */
  def compose(mapOp: MapOperation): MapOperation = new MapOperation {
    def applyToModel(model: DataType): Either[LatisException, DataType] =
      mapOp.applyToModel(model).flatMap(self.applyToModel)

    def mapFunction(model: DataType): Sample => Sample = {
      val tmpModel = mapOp.applyToModel(model).fold(throw _, identity)
      mapOp.mapFunction(model).andThen(self.mapFunction(tmpModel))
    }
  }

}

object MapOperation {

  def unapply(mapOp: MapOperation): Option[DataType => Sample => Sample] =
    Some(mapOp.mapFunction)
}
