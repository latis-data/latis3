package latis.input

import cats.effect.IO
import fs2.Stream
import java.net.URI

/**
 * Trait for a data source that provides data as an fs2.Stream of Bytes
 * for a specific URI scheme. StreamUtils.getStream(uri) will find the appropriate 
 * implementation for a given URI.
 */
trait StreamSource {
  
  /**
   * Does this StreamSource support the given URI schema (e.g. "http", "s3").
   */
  def supportsScheme(uriScheme: String): Boolean
  
  /**
   * Generate an fs2.Stream for the data identified by the given URI.
   */
  def getStream(uri: URI): Stream[IO, Byte]
}
