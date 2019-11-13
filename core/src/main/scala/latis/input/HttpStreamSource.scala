package latis.input

import latis.util.NetUtils
import latis.util.StreamUtils.blockingExecutionContext
import latis.util.StreamUtils.contextShift

import java.io.InputStream
import java.net.URI

import cats.effect.IO
import fs2.io.readInputStream
import fs2.Stream

import org.http4s.client._
import org.http4s.client.blaze._
import org.http4s.Request
import org.http4s.Uri
import scala.concurrent.ExecutionContext.Implicits.global


/**
 * Creates an StreamSource from a "http" or "https" URI.
 * This uses the http4s client library to request a Stream.
 * The resource will be released automatically after it is
 * consumed or after a timeout (1m).
 */
class HttpStreamSource extends StreamSource {
  
  /**
   * The UrlStreamSource supports URIs that can be simply converted to URLs:
   * "file", "http", and "https".
   */
  def supportsScheme(uriScheme: String): Boolean =
    List("http", "https").contains(uriScheme)
  
  /**
   * Return a Stream of Bytes (in IO) from the provided URI.
   */
  def getStream(uri: URI): Option[Stream[IO, Byte]] = {
    if (uri.isAbsolute && supportsScheme(uri.getScheme)) {
      val request = Request[IO](uri = Uri.unsafeFromString(uri.toString))
      val stream = BlazeClientBuilder[IO](global).stream.flatMap {
        _.stream(request).flatMap(_.body)
      }
      Option(stream)
    }
    else None
  }
  
}
