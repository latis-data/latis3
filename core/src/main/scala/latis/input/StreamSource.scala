package latis.input

import cats.effect.IO
import fs2.Stream

/**
 * Trait for a data source that provides data as an fs2 Stream of Bytes.
 */
trait StreamSource {
  def getStream: Stream[IO, Byte]
}
