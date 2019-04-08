package latis.output

import java.io.OutputStream

import scala.concurrent.ExecutionContext

import cats.Applicative
import cats.effect.ContextShift
import cats.effect.Sync
import cats.implicits._
import fs2.Pipe

import latis.util.StreamUtils

/**
 * Write to an output stream.
 */
class OutputStreamWriter[F[_]: ContextShift: Sync](
  outputStream: F[OutputStream],
  blockingEC: ExecutionContext
) extends Writer[F, Byte] {

  override val write: Pipe[F, Byte, Unit] =
    fs2.io.writeOutputStream(outputStream, blockingEC)
}

object OutputStreamWriter {

  /**
   * Create a writer from an impure output stream.
   *
   * This method uses the default blocking execution context in
   * `latis.util.StreamUtils`.
   */
  def unsafeFromOutputStream[F[_]: Applicative: ContextShift: Sync](
    os: OutputStream
  ): OutputStreamWriter[F] =
    new OutputStreamWriter(os.pure[F], StreamUtils.blockingExecutionContext)
}
