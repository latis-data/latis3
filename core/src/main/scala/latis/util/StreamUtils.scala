package latis.util

import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors
import cats.effect.IO
import cats.effect.ContextShift
import fs2._

/**
 * Utilities for working with fs2 Streams.
 */
object StreamUtils {
  
  /**
   * Provide a single blocking ExecutionContext for use with
   * fs2.io.readInputStream (e.g. UrlStreamSource).
   */
  val blockingExecutionContext: ExecutionContext = 
    ExecutionContext.fromExecutorService(Executors.newCachedThreadPool)

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
    Stream.emits(seq).flatMap(x => Stream.eval( IO(x) ))
    
  /**
   * Unsafely turn an fs2.Stream into a Seq.
   */
  def unsafeStreamToSeq[T](stream: Stream[IO, T]): Seq[T] = 
    stream.compile.toVector.unsafeRunSync
    
}