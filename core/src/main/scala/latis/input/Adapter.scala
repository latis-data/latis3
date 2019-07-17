package latis.input

import latis.data.SampledFunction

import java.net.URI

/**
 * An Adapter provides access to data in the form of a 
 * SampledFunction given a URI.
 */
trait Adapter {
  
  def apply(uri: URI): SampledFunction
}

