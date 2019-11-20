package latis.util

import java.io.File
import java.net.URI
import java.net.URL
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.util.Properties
import scala.util.Try

/**
 * Defines utility functions for working with files.
 */
object FileUtils {
  
  /**
   * Optionally returns an absolute path for the given path.
   * If the given path is relative, this looks in the classpath
   * and current working directory.
   */
  def resolvePath(path: Path): Option[Path] = {
    if (path.isAbsolute) {
      Some(path)
    } else getClass.getResource(File.separator + path) match {
      case url: URL => url.getProtocol match {
        case "file" =>
          Some(Paths.get(url.toURI))
        case "jar" =>
          val Array(fsUri, file) = url.toString.split("!")
          val env = new java.util.HashMap[String,String]()
          val fs = FileSystems.newFileSystem(URI.create(fsUri), env)
          Some(fs.getPath(file))
        case p => 
          val msg = s"Not able to resolve path for unknown protocol $p"
          throw new RuntimeException(msg)
      }
      case null => 
        // Not found in classpath, try the current working directory
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
   * Searches a given directory for files of a given extension and returns the
   * filenames.
   * @param path path to search for files
   * @param ext extension of files to filter on
   */
  def getFileList(path: Path, ext: String): Try[Seq[Path]] = {
    Try {
      val fileIter = Files.list(path).iterator().asScala
      fileIter.filter(_.toString.endsWith(ext)).toList
    }
  }
}
