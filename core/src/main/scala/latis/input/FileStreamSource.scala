package latis.input

import latis.util.NetUtils
import latis.util.StreamUtils.blockingExecutionContext
import latis.util.StreamUtils.contextShift

import java.io.InputStream
import java.net.URI

import cats.effect.IO
import fs2.Stream
import fs2.io.readInputStream

/**
 * Creates a StreamSource from a "file" URI.
 * This will resolve and convert the URI to a URL
 * and open an InputStream.
 */
class FileStreamSource extends StreamSource {
  //TODO: confirm that resource is released, even if we don't hit the EOF

  /**
   * The FileStreamSource supports relative or absolute file URIs.
   */
  def supportsScheme(uriScheme: String): Boolean =
    uriScheme == "file"
  
  /**
   * Return a Stream of Bytes (in IO) from the provided URI.
   */
  def getStream(uri: URI): Option[Stream[IO, Byte]] = {
    // Make sure the URI is absolute
    val resolvedUri = NetUtils.resolveUri(uri) getOrElse {
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
