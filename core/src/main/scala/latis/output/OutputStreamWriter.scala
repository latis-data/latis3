package latis.output

import java.io.OutputStream

import cats.effect.Sync
import cats.syntax.all._
import fs2.Pipe

/**
 * Write to an output stream.
 */
class OutputStreamWriter[F[_]: Sync](
  outputStream: F[OutputStream]
) extends Writer[F, Byte] {

  override val write: Pipe[F, Byte, Unit] =
    fs2.io.writeOutputStream(outputStream)
}

object OutputStreamWriter {

  /**
   * Create a writer from an impure output stream.
   */
  def unsafeFromOutputStream[F[_]: Sync](
    os: OutputStream
  ): OutputStreamWriter[F] =
    new OutputStreamWriter(os.pure[F])
}
