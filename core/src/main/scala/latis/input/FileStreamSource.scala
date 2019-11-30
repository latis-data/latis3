package latis.input

import java.io.InputStream
import java.net.URI

import cats.effect.IO
import fs2.Stream
import fs2.io.readInputStream
import latis.util.NetUtils
import latis.util.StreamUtils.blockingExecutionContext
import latis.util.StreamUtils.contextShift

/**
 * Creates a StreamSource from a relative or absolute file URI.
 * This will resolve and convert the URI to a URL
 * and open an InputStream.
 */
class FileStreamSource extends StreamSource {
  //TODO: confirm that resource is released, even if we don't hit the EOF

  /**
   * Specifies that a "file" URI can be read by this StreamSource.
   */
  def supportsScheme(uriScheme: String): Boolean =
    uriScheme == "file"
  
  /**
   * Returns a Stream of Bytes (in IO) from the provided URI.
   */
  def getStream(uri: URI): Option[Stream[IO, Byte]] = {
    // Make sure the URI is absolute
    val resolvedUri = NetUtils.resolveUri(uri) getOrElse {
      //TODO: leftMap
      throw new RuntimeException(s"Unable to resolve URI: $uri")
    }
    if (supportsScheme(resolvedUri.getScheme)) {
      //Note that opening the InputStream will be delayed.
      val fis: IO[InputStream] = IO(resolvedUri.toURL.openStream)
      val chunkSize: Int = 4096 //TODO: tune? config option?
      Option(readInputStream[IO](fis, chunkSize, blockingExecutionContext))
    }
    else None
  }
  
}
