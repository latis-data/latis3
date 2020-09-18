package latis.input

import java.net.URI

import latis.data.Data
import latis.ops.Operation

/**
 * An Adapter provides access to data in the form of a
 * SampledFunction given a URI.
 */
trait Adapter {
  //TODO: add SmartAdapter that takes ops?

  def canHandleOperation(op: Operation): Boolean = false

  def getData(baseUri: URI, ops: Seq[Operation] = Seq.empty): Data
}
