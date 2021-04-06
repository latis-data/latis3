package latis.output

import fs2.Pipe

/** A wrapper around `Pipe[F, I, Unit]`. */
trait Writer[F[_], I] {
  val write: Pipe[F, I, Unit]
}
