package latis.output

import java.io.OutputStream

import cats.effect.IO
import fs2._

import latis.dataset.Dataset
import latis.util.StreamUtils._

case class TextWriter(out: OutputStream) {
  def write(dataset: Dataset): Unit =
    new TextEncoder()
      .encode(dataset)
      .through(text.utf8Encode)
      .through(OutputStreamWriter.unsafeFromOutputStream[IO](out).write)
      .compile
      .drain
      .unsafeRunSync()
}

object TextWriter {
  def apply(): TextWriter = TextWriter(System.out)
}
