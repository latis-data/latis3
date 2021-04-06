package latis.util

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

import cats.effect.Blocker
import cats.effect.ContextShift
import cats.effect.IO
import fs2.Stream

/**
 * Utilities for working with fs2 Streams.
 */
object StreamUtils {
  /**
   * An execution context for blocking operations.
   */
  val blocker: Blocker =
    Blocker.liftExecutorService(Executors.newCachedThreadPool())

  /**
   * Provide an implicit ContextShift for use with
   * fs2.io.readInputStream (e.g. UrlStreamSource).
   */
  implicit val contextShift: ContextShift[IO] =
    IO.contextShift(ExecutionContext.global)

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
