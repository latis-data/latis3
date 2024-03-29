package latis.input

import java.net.URI

import cats.effect.IO
import fs2.Stream
import fs2.io.file.Files
import fs2.io.file.Path

import latis.util.LatisException
import latis.util.NetUtils

/**
 * Creates a StreamSource from a relative or absolute file URI.
 */
class FileStreamSource extends StreamSource {
  //TODO: enforce that resource is released, even if we don't hit the EOF

  /**
   * Returns a Stream of Bytes (in IO) from the provided URI.
   * This method is used by the StreamSource service provider.
   */
  def getStream(uri: URI): Option[Stream[IO, Byte]] =
    // If this is not an absolute file URI, try to resolve it as a file.
    if (uri.isAbsolute && uri.getScheme != "file") None //we don't handle it
    else
      NetUtils.resolveUri(uri) match {
        case Left(le) =>
          val msg = s"Failed to resolve file URI: $uri"
          Option(Stream.raiseError[IO](LatisException(msg, le)))
        case Right(u) => Option(Files[IO].readAll(Path(u.getPath))) //may raise error in stream
      }

}
