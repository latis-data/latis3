package latis.output

import java.io.OutputStream

import cats.Applicative
import cats.effect.Blocker
import cats.effect.ContextShift
import cats.effect.Sync
import cats.syntax.all._
import fs2.Pipe

import latis.util.StreamUtils

/**
 * Write to an output stream.
 */
class OutputStreamWriter[F[_]: ContextShift: Sync](
  outputStream: F[OutputStream],
  blocker: Blocker
) extends Writer[F, Byte] {

  override val write: Pipe[F, Byte, Unit] =
    fs2.io.writeOutputStream(outputStream, blocker)
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
    new OutputStreamWriter(os.pure[F], StreamUtils.blocker)
}
