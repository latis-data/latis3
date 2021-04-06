package latis.input

import java.net.URI

import cats.effect.IO
import fs2.Stream
import fs2.ftp.FtpSettings._
import fs2.ftp.UnsecureFtp._

import latis.util.LatisException
import latis.util.NetUtils._
import latis.util.StreamUtils._

/**
 * Creates an StreamSource from a "ftp" or "ftps" URI.
 * This uses the fs2-ftp client library to request a Stream.
 * The resource will be released automatically after it is
 * consumed or after a timeout.
 */
class FtpStreamSource extends StreamSource {
  /**
   * Specifies that "ftp" or "ftps" URIs can be read by this StreamSource.
   */
  def supportsScheme(uriScheme: String): Boolean =
    List("ftp", "ftps").contains(uriScheme)

  /**
   * Generate an fs2.Stream for the data identified by the given URI.
   * If this StreamSource does not support the given URI scheme,
   * return None. Other errors can be raised in IO.
   */
  def getStream(uri: URI): Option[Stream[IO, Byte]] =
    if (uri.isAbsolute && supportsScheme(uri.getScheme)) {
      val stream = for {
        host <- getHost(uri)
        port <- getPortOrDefault(uri)
        userInfo = getUserInfo(uri)
          .getOrElse(("anonymous", ""))
        credentials = FtpCredentials(userInfo._1, userInfo._2)
        settings = if (uri.getScheme == "ftps")
          UnsecureFtpSettings.ssl(host, port, credentials)
        else UnsecureFtpSettings(host, port, credentials)
        path <- Option(uri.getPath)
          .filter(_.nonEmpty)
          .toRight(LatisException(s"Could not get path from uri: $uri"))
      } yield Stream.resource(connect[IO](settings)).flatMap(_.readFile(path))
      Some(stream.fold(Stream.raiseError[IO], identity))
    } else None
}
