package latis.ops

import cats.effect.IO
import cats.syntax.all.*
import fs2.Pipe

import latis.data.Data
import latis.data.Sample
import latis.data.StreamFunction
import latis.model.DataType
import latis.util.LatisException

/**
 * Defines an Operation that can be applied one Sample at a time.
 * This Operation can be applied to a Stream of Samples as a Pipe.
 * These Operations can be composed with a MapOperation.
 */
trait StreamOperation extends UnaryOperation {

  /**
   * Returns a Pipe that can be applied to a Stream of Samples.
   */
  def pipe(model: DataType): Pipe[IO, Sample, Sample]

  /**
   * Composes this StreamOperation with the given MapOperation.
   * The MapOperation will be applied first.
   */
  //def compose(mapOp: MapOperation): StreamOperation
  //TODO: simply compose pipes? but need to be able to build Op of original type

  /**
   * Applies this operation to Data.
   */
  def applyToData(data: Data, model: DataType): Either[LatisException, Data] =
    StreamFunction(data.samples.through(pipe(model))).asRight

}
