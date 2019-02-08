package latis.util

import java.io.File
import java.net.URL
import java.net.URLDecoder
import java.nio.file.Files

object FileUtils {
  
  /**
   * Return the full file system path for the given relative path.
   * If the path is already fully resolved, just return it.
   */
  def resolvePath(path: String): Option[String] = {
    if (path.startsWith(File.separator)) Some(path)
    //try classpath
    else getClass.getResource(File.separator + path) match {
      case url: URL => Some(URLDecoder.decode(url.getPath, "UTF-8"))
      //else try the current working directory
      case null => 
        val fpath = scala.util.Properties.userDir + File.separator + path
        if (new File(fpath).exists) Some(fpath)
        else None
    }
  }
}