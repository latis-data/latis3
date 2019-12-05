package latis.input

import java.net.URI

import cats.effect.IO
import fs2.Stream

import latis.data._
import latis.ops.Operation

/**
 * Defines an Adapter using record semantics to read data.
 * It converts an effectful Stream of records (R) to an
 * effectful Stream of Samples.
 */
trait StreamingAdapter[R] extends Adapter {

  /**
   * Provides a Stream of records.
   */
  def recordStream(uri: URI): Stream[IO, R]

  /**
   * Optionally parse a record into a Sample
   */
  def parseRecord(r: R): Option[Sample]

  /**
   * Implements the Adapter interface using record semantics.
   * Note that this approach is limited to a single traversal.
   */
  def getData(uri: URI, ops: Seq[Operation] = Seq.empty): SampledFunction =
    StreamFunction(recordStream(uri).map(parseRecord).unNone)

}
