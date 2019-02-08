package latis.input

import latis.util.FileUtils
import java.io.File

/**
 * Find a DatasetSource based on an FDML descriptor.
 */
class FdmlDatasetSourceProvider {
  
  /**
   * Return a DatasetSource for a Dataset with the given identifier.
   */
  def getDatasetSource(dsid: String): Option[DatasetSource] = {
    val dir = "datasets" //TODO: config
    FileUtils.resolvePath(dir + File.separator + dsid + ".fdml")
      .map(path => FDMLReader(path))
  }
}