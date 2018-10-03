package latis.input

import latis.util.StreamUtils._

import java.io.InputStream
import java.net.URL

import cats.effect.IO
import fs2.Stream
import fs2.io.readInputStream

/**
 * Create an StreamSource from a URL.
 * The resource will be released automatically.
 */
case class UrlStreamSource(val url: URL) extends StreamSource {
  
  /**
   * Return a Stream of Bytes (in IO) from the provided URL.
   */
  def getStream: Stream[IO, Byte] = {
    //Note that opening the InputStream will be delayed.
    val fis: IO[InputStream] = IO(url.openStream)
    val chunkSize: Int = 4096 //TODO: tune? config option?
    readInputStream[IO](fis, chunkSize, blockingExecutionContext)
  }
  
}
