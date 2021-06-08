package latis.util

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream

/**
 * Utilities for working with fs2 Streams.
 */
object StreamUtils {

  /**
   * Put a Seq of some type into a Stream (in IO).
   */
  def seqToIOStream[T](seq: Seq[T]): Stream[IO, T] =
    Stream.emits(seq).flatMap(x => Stream.eval(IO(x)))

  /**
   * Unsafely turn an fs2.Stream into a Seq.
   */
  def unsafeStreamToSeq[T](stream: Stream[IO, T]): Seq[T] =
    stream.compile.toVector.unsafeRunSync()

  /**
   * Returns the first sample of the given stream
   * by running it unsafely.
   */
  def unsafeHead[T](stream: Stream[IO, T]): T =
    stream.head.compile.toVector.unsafeRunSync().head

}
