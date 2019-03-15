package latis.input

import latis.util.FileUtils
import java.io.File
import java.net.URI
import latis.model.Dataset

/**
 * Attempt to find a Dataset by mapping the id to a FDML descriptor.
 */
class FDMLDatasetResolver {
  
  /**
   * Optionally return a Dataset with the given identifier if a FDML
   * definition can be found.
   * This method is used by the DatasetResolver ServiceLoader to determine
   * if this can provide the requested Dataset.
   */
  def getDataset(id: String): Option[Dataset] = {
    val dir = "datasets" //TODO: config
    FileUtils.resolvePath(dir + File.separator + id + ".fdml") map { path =>
      FDMLReader(new URI("file://" + path)).getDataset
    }
  }
}
