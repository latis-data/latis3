package latis.util

import java.io.File
import java.net.URI
import java.net.URL
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.Files
import scala.collection.JavaConverters._

import scala.util.Properties

/**
 * Utility functions for working with files.
 */
object FileUtils {
  
  /**
   * Optionally return an absolute path for the given path.
   * If the given path is relative, look in the classpath
   * and current working directory.
   */
  def resolvePath(path: Path): Option[Path] = {
    if (path.isAbsolute) {
      // Already absolute
      Some(path)
    } else getClass.getResource(File.separator + path) match {
      // Try classpath
      case url: URL => 
        // Found it
        Some(Paths.get(url.getPath))
      case null => 
        // Not found, try the current working directory
        val fpath = Paths.get(Properties.userDir, path.toString)
        // Make sure it exists since this is our last attempt
        if (fpath.toFile.exists) Some(fpath)
        else None
    }
  }
  
  def resolvePath(path: String): Option[Path] = 
    resolvePath(Paths.get(path))
  
    
  //TODO: NetUtils?
  def resolveUri(uri: URI): Option[URI] = {
    if (uri.isAbsolute) {
      // Already complete with scheme
      Some(uri) 
    } else resolvePath(uri.getPath).map(_.toUri)
  }
  
  def resolveUri(uri: String): Option[URI] = 
    resolveUri(new URI(uri))


  /**
   * returns a list of filenames with the extension striped
   * @param path
   *             path to search for files
   * @param ext
   *            extension of files to search (can be regular expression)
   */
  def getFileList(path: String, ext: String): Seq[String] = {
    val file_iter = Files.list(Paths.get(path)).iterator().asScala
    val extLen = ext.length + 1  // add one for the "."
    file_iter.map(p => p.getFileName.toString)
      .filter(p => p.endsWith(ext))
      .map(s => s.substring(0, s.length - extLen))
      .toList
  }
}
