package latis.ops

import cats.effect.IO
import cats.effect.Ref
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
   *
   * This will drop any Sample that results in an Exception
   * and print an error message for every nth error when n
   * is a power of 2.
   */
  def pipe(model: DataType): Pipe[IO, Sample, Sample] = {
    val f = mapFunction(model)
    (stream: Stream[IO, Sample]) =>
      Stream.eval(Ref[IO].of(0)).flatMap { cntRef =>
        stream.map { sample =>
          Either.catchNonFatal(f(sample))
        }.evalTapChunk {
          case Left(t) =>
            cntRef.updateAndGet(_ + 1).flatMap { cnt =>
              if ((cnt & (cnt - 1)) == 0) { // true for powers of 2
                val msg = s"MapOperation warning #$cnt: Sample dropped. $t"
                IO.println(msg) //TODO: log
              } else IO.unit
            }
          case _ => IO.unit
        }.collect {
          case Right(s) => s
        }
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
