package latis.ops

import cats.effect.IO
import fs2.Pipe
import fs2.Stream

import latis.data._
import latis.model._

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
   */
  def pipe(model: DataType): Pipe[IO, Sample, Sample] =
    (stream: Stream[IO, Sample]) => stream.map(mapFunction(model))

  /**
   * Composes this operation with the given MapOperation.
   * Note that the given Operation will be applied first.
   */
  def compose(mapOp: MapOperation): MapOperation = new MapOperation {
    def applyToModel(model: DataType): DataType =
      self.applyToModel(mapOp.applyToModel(model))

    def mapFunction(model: DataType): Sample => Sample = {
      val tmpModel = mapOp.applyToModel(model)
      mapOp.mapFunction(model).andThen(self.mapFunction(tmpModel))
    }
  }

}

object MapOperation {

  def unapply(mapOp: MapOperation): Option[DataType => Sample => Sample] =
    Some(mapOp.mapFunction)
}
