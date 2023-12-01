package latis.input

import java.net.URI
import java.util.ServiceLoader

import scala.jdk.CollectionConverters._

import cats.effect.IO
import fs2.Stream

import latis.util.LatisException

/**
 * Trait for a data source that provides data as an fs2.Stream of Bytes in IO.
 * StreamSource.getStream(uri) will find the appropriate implementation for a
 * given URI or raise an error in IO.
 * The intent is to provide StreamSource implementations that support a given
 * URI scheme but we might be able to use this to support multiple (backup)
 * sources for a dataset. Thus a StreamSource implementation only needs to
 * check if it supports the given URI scheme. It does not have to try to see
 * if the source is valid.
 */
trait StreamSource {
  //TODO: return Option[Either[LatisException, Stream...]] so we can capture errors before streaming?

  /**
   * Generate an fs2.Stream for the data identified by the given URI.
   * If this StreamSource does not support the given URI scheme,
   * return None. Other errors can be raised in IO.
   */
  def getStream(uri: URI): Option[Stream[IO, Byte]]
}

object StreamSource {

  /**
   * Given a URI for a data source, return an fs2.Stream.
   * This will inspect StreamSource implementations listed
   * in META-INF/services/ of jar files in the classpath.
   */
  def getStream(uri: URI): Stream[IO, Byte] =
    ServiceLoader
      .load(classOf[StreamSource])
      .asScala
      .flatMap(_.getStream(uri))
      .headOption
      .getOrElse {
        val msg = s"Failed to find a StreamSource for URI: $uri"
        Stream.raiseError[IO](LatisException(msg))
      }

}
