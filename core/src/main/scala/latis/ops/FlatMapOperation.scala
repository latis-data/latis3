package latis.ops

import cats.effect.IO
import fs2.Pipe
import fs2.Stream

import latis.data.*
import latis.model.*

/**
 * Defines an Operation that maps a function of Sample => MemoizedFunction
 * over the data of a Dataset to generate a new Dataset.
 * The MemoizedFunction resulting from each Sample will be flattened
 * into a stream of Samples.
 */
trait FlatMapOperation extends StreamOperation {

  /**
   * Defines a function that generates a SampledFunction
   * from a Sample.
   */
  def flatMapFunction(model: DataType): Sample => MemoizedFunction

  /**
   * Implements a Pipe in terms of the flatMapFunction.
   */
  def pipe(model: DataType): Pipe[IO, Sample, Sample] =
    (stream: Stream[IO, Sample]) =>
      stream.flatMap(flatMapFunction(model)(_).samples)

}
