package latis.input

import java.net.URI

import scala.annotation.nowarn

import latis.data.Data
import latis.ops.Operation

/**
 * An Adapter provides access to data in the form of a
 * SampledFunction given a URI.
 */
trait Adapter {
  //TODO: add SmartAdapter that takes ops?
  //TODO: require UnaryOperations

  def canHandleOperation(@nowarn("cat=unused") op: Operation): Boolean = false

  def getData(baseUri: URI, ops: Seq[Operation] = Seq.empty): Data
}
