package latis.input

import latis.util.StreamUtils.blockingExecutionContext
import latis.util.StreamUtils.contextShift

import java.io.InputStream
import java.net.URI

import cats.effect.IO
import fs2.Stream
import fs2.io.readInputStream

/**
 * Create an StreamSource from a "file", "http", or "https" URI.
 * This will simply convert the URI to a URL and open an InputStream.
 * The resource will be released automatically.
 */
class UrlStreamSource extends StreamSource {
  
  /**
   * The UrlStreamSource supports URIs that can be simply converted to URLs:
   * "file", "http", and "https".
   */
  def supportsScheme(uriScheme: String): Boolean =
    List("file", "http", "https").contains(uriScheme)
  
  /**
   * Return a Stream of Bytes (in IO) from the provided URI.
   */
  def getStream(uri: URI): Option[Stream[IO, Byte]] = {
    if (supportsScheme(uri.getScheme)) {
      //TODO: put logic in StreamUtils? keep InputStream lazy
      //Note that opening the InputStream will be delayed.
      val fis: IO[InputStream] = IO(uri.toURL.openStream)
      val chunkSize: Int = 4096 //TODO: tune? config option?
      Option(readInputStream[IO](fis, chunkSize, blockingExecutionContext))
    }
    else None
  }
  
}
