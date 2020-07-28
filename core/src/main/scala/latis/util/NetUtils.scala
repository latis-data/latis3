package latis.util

import java.net.URI
import java.nio.file.Path
import java.nio.file.Paths

import cats.implicits._
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
}
