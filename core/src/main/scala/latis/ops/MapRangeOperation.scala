package latis.ops

import cats.effect.IO
import fs2.Pipe
import fs2.Stream

import latis.data.*
import latis.model.*

/**
 * Defines an Operation that maps a function of Data => Data
 * over the data of a Dataset to generate a new Dataset
 * with only the range of each Sample modified. The resulting
 * Dataset will preserve the original domain set.
 */
trait MapRangeOperation extends StreamOperation {
  //TODO: just a MapOp applied to the range

  /**
   * Defines a function that modifies only range Data.
   */
  def mapFunction(model: DataType): Data => Data

  /**
   * Implements a Pipe in terms of the mapFunction.
   */
  def pipe(model: DataType): Pipe[IO, Sample, Sample] =
    (stream: Stream[IO, Sample]) => stream.map { sample =>
      val range = RangeData(mapFunction(model)(Data.fromSeq(sample.range)))
      Sample(sample.domain, range)
    }

}
