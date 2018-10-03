package latis.data

import fs2.Stream
import cats.effect.IO
import scala.util.Try

/**
 * Implement a SampledFunction with a Stream of Samples.
 * Note that evaluation of a StreamFunction is limited by
 * being traversable once.
 * A Dataset can be memoized with "force" to ensure that it has a
 * MemoizedFunction that can be more generally evaluated.
 */
case class StreamFunction(samples: Stream[IO, Sample]) extends SampledFunction {
  
  /**
   * Traverse the Stream of Samples looking for a match.
   * Fail if no match is found.
   */
  def apply(v: DomainData): Stream[IO, RangeData] =
   samples.find(_._1 == v).map(_._2)  //TODO: what will the error case look like?

}
