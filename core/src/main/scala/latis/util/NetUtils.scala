package latis.util

import java.net.URI
import java.net.URLDecoder
import java.nio.file.Path
import java.nio.file.Paths

import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import fs2.text

import latis.input.StreamSource

object NetUtils {

  /** Returns a Path corresponding to the given file URI. */
  def getFilePath(uri: URI): Either[LatisException, Path] =
    if (uri.getScheme() != "file") {
      LatisException("Only file URIs are supported").asLeft
    } else Paths.get(uri).asRight

  /**
   * Creates a URI from the given string.
   */
  def parseUri(uri: String): Either[LatisException, URI] =
    Either.catchNonFatal {
      new URI(uri)
    }.leftMap {
      LatisException(s"Failed to parse URI: $uri", _)
    }

  def resolveUri(uri: URI): Either[LatisException, URI] =
    if (uri.isAbsolute) Either.right(uri) //Already complete with scheme
    else
      Either.catchNonFatal {
        //TODO: either-ify FileUtils
        FileUtils.resolvePath(uri.getPath) match {
          case Some(p) => p.toUri
          case None    => throw LatisException("FileUtils failed to resolve path")
        }
      }.leftMap {
        LatisException(s"Failed to resolve URI: $uri", _)
      }

  def resolveUri(uri: String): Either[LatisException, URI] =
    parseUri(uri).flatMap(resolveUri)

  def readUriIntoString(uri: URI): Either[LatisException, String] =
    StreamSource
      .getStream(uri)
      .through(text.utf8Decode)
      .compile
      .string
      .attempt
      .unsafeRunSync()
      .leftMap {
        LatisException(s"Failed to read URI: $uri", _)
      }

  /** Returns the host specified in the URI */
  def getHost(uri: URI): Either[LatisException, String] =
    Option(uri.getHost).toRight(LatisException(s"Couldn't parse host from uri: $uri"))

  /** Returns the port number specified in the URI. */
  def getPort(uri: URI): Either[LatisException, Int] =
    Option(uri.getPort)
      .filter(_ != -1)
      .toRight(LatisException(s"Couldn't parse port from uri: $uri"))

  /**
   * Returns the port number specified in the URI. If no port is specified, the
   * uri scheme is used to get a default port.
   */
  def getPortOrDefault(uri: URI): Either[LatisException, Int] =
    getPort(uri).orElse {
      Option(uri.getScheme) match {
        case None => LatisException(s"Couldn't parse port or scheme from uri: $uri").asLeft
        case Some(scheme) => scheme match {
          case "http" => 80.asRight
          case "https" => 443.asRight
          case "ftp" | "ftps" => 21.asRight
          case "ssh" | "sftp" => 22.asRight
          case _ => LatisException(s"No default port for scheme $scheme").asLeft
        }
      }
    }

  /**
   * Returns a tuple of user and password. Returns Left if no user is specified,
   * or if user info can't be parsed. Returns "" (empty string) for password if
   * user is specified without a password. Any colon (:), at sign (@), or forward
   * slash (/) in the user or password must be encoded with "%3A", "%40", or
   * "%2F" respectively.
   */
  def getUserInfo(uri: URI): Either[LatisException, (String, String)] =
    Option(uri.getRawUserInfo) match {
      case None => LatisException(s"Couldn't parse user info from uri: $uri").asLeft
      case Some(rawInfo) => rawInfo.split(':').toList match {
        case rawUser :: rawPw :: Nil => (
          URLDecoder.decode(rawUser, "UTF-8"),
          URLDecoder.decode(rawPw, "UTF-8")
        ).asRight
        case _ => LatisException(s"Couldn't parse user and password from user-info: $rawInfo").asLeft
      }
    }
}
