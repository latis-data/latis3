package latis.input

import java.net.URI

import cats.effect.IO
import fs2.Stream
import org.http4s.Request
import org.http4s.Uri
import org.http4s.client.middleware.FollowRedirect
import org.http4s.ember.client.EmberClientBuilder

/**
 * Creates an StreamSource from a "http" or "https" URI.
 * This uses the http4s client library to request a Stream.
 * The resource will be released automatically after it is
 * consumed or after a timeout (default 1m).
 */
class HttpStreamSource extends StreamSource {

  /**
   * Specifies that "http" or "https" URIs can be read by this StreamSource.
   */
  def supportsScheme(uriScheme: String): Boolean =
    List("http", "https").contains(uriScheme)

  /**
   * Returns a Stream of Bytes (in IO) from the provided URI.
   */
  def getStream(uri: URI): Option[Stream[IO, Byte]] =
    if (uri.isAbsolute && supportsScheme(uri.getScheme)) {
      val request = Request[IO](uri = Uri.unsafeFromString(uri.toString))
      val client = EmberClientBuilder.default[IO]
        .withMaxResponseHeaderSize(8192)
        .build
      val stream = Stream.resource(client)
        .map(FollowRedirect(maxRedirects = 3))
        .flatMap(_.stream(request))
        .flatMap(_.body)
      Option(stream)
    } else None

}
