package latis.input

import latis.data._
import java.net.URI
import latis.data._
import cats.effect.IO
import fs2._

/**
 * A StreamingAdapter uses record semantics to read data.
 * It converts an effectful Stream of records (R) to an
 * effectful Stream of Samples.
 */
trait StreamingAdapter[R] extends Adapter {
  
  /**
   * Provide a Stream of records.
   */
  def recordStream(uri: URI): Stream[IO, R] 
  
  /**
   * Optionally parse a record into a Sample
   */
  def parseRecord(r: R): Option[Sample]
  
  /**
   * Implement the Adapter interface using record semantics.
   * Note that this approach is limited to a single traversal.
   */
  def apply(uri: URI): SampledFunction =
    StreamFunction(recordStream(uri).map(parseRecord(_)).unNone)

}
