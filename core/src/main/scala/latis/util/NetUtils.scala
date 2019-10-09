package latis.util

import java.io.File
import java.net.URI
import java.net.URL
import java.nio.file.Path
import java.nio.file.Paths

import scala.util.Properties

/**
 * Utility functions for working with URIs.
 */
object NetUtils {
  
  /**
   * Optionally return a URI by resolving the given URI
   * which may be a relative path to a file.
   */
  def resolveUri(uri: URI): Option[URI] = {
    if (uri.isAbsolute) {
      // Already complete with scheme
      Some(uri) 
    } else FileUtils.resolvePath(uri.getPath).map(_.toUri)
  }
  
  def resolveUri(uri: String): Option[URI] = 
    resolveUri(new URI(uri))
}
