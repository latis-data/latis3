package latis.input

import latis.util.FileUtils
import java.io.File
import java.net.URI
import latis.model.Dataset

/**
 * Find a Dataset based on an FDML descriptor.
 */
class FDMLDatasetSource {
  
  /**
   * Return a Dataset with the given identifier if a FDML
   * definition can be found.
   * This method is used by the DatasetSource ServiceLoader to determine
   * if this can provide the requested Dataset.
   */
  def getDataset(name: String): Option[Dataset] = {
    val dir = "datasets" //TODO: config
    FileUtils.resolvePath(dir + File.separator + name + ".fdml") map { path =>
      FDMLReader(new URI("file://" + path)).getDataset
    }
  }
}
