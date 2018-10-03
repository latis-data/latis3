package latis.util

import latis.input._

import java.net.URI

object NetUtils {
  
  /**
   * Given a URI of a data source, use the scheme
   * to create a StreamSource that can provide an 
   * fs2 Stream for that resource.
   */
  //TODO: should we just return the Stream? in IO so delayed
  def resolve(uri: URI): StreamSource = uri.getScheme match {
    case "http" | "https" | "file" => UrlStreamSource(uri.toURL) 
    case _ => ??? //unsupported scheme
  }
}