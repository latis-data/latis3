package latis.ops

import cats.effect.IO
import cats.syntax.all.*
import fs2.Pipe
import fs2.Stream

import latis.data.*
import latis.model.*
import latis.util.LatisException

/**
 * A Filter is a unary Operation that applies a boolean
 * predicate to each Sample of the given Dataset resulting
 * in a new Dataset that has all the "false" Samples removed.
 * This only impacts the number of Samples in the Dataset.
 * It does not affect the model.
 * A Filter operation is idempotent.
 */
trait Filter extends UnaryOperation with StreamOperation { self =>
  //TODO: update "length" metadata?
  //TODO: clarify behavior of nested Functions: all or none

  /**
   * Provides a no-op implementation for Filters.
   */
  def applyToModel(model: DataType): Either[LatisException, DataType] = Right(model)

  /**
   * Defines a function that specifies whether a Sample
   * should be kept.
   */
  def predicate(model: DataType): Either[LatisException, Sample => Boolean]

  /**
   * Implements a Pipe in terms of the predicate.
   */
  def pipe(model: DataType): Pipe[IO, Sample, Sample] =
    (stream: Stream[IO, Sample]) => predicate(model) match {
      case Right(p) => stream.filter(p)
      case Left(le) => Stream.raiseError[IO](le)
    }

  /**
   * Composes this operation with the given MappingOperation.
   * Note that the given operation will be applied first.
   */
  def compose(mapOp: MapOperation): Filter = (model: DataType) =>
    mapOp.applyToModel(model)                // model after mapOp application
      .flatMap(predicate)                    // predicate with that model
      .map(mapOp.mapFunction(model).andThen) // compose predicate with map function

}

object Filter {
  def apply(f: Sample => Boolean): Filter = new Filter {
    def predicate(model: DataType): Either[LatisException, Sample => Boolean] = f.asRight
  }
}
