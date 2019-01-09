package latis.data

import fs2.Stream
import cats.effect.IO

/**
 * Implement a SampledFunction with an fs2.Stream of Samples.
 * Note that evaluation of a StreamFunction is limited by
 * being traversable once.
 * A Dataset can be memoized with "force" to ensure that it has a
 * MemoizedFunction that can be more generally evaluated.
 */
case class StreamFunction(streamSamples: Stream[IO, Sample]) extends SampledFunction {
  
  /*
   * TODO: can/should we support an empty Stream?
   * fs2.Stream doesn't define isEmpty but you can get
   *   Stream.empty: Stream[F, INothing]
   */
  
  /**
   * We can't safely determine if a Stream is empty
   * so we must assume that it is not.
   */
  def isEmpty = false
}
