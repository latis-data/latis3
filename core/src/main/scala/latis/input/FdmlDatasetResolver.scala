package latis.input

import latis.util._
import java.io.File
import java.net.URI
import latis.model.Dataset
import java.nio.file.Paths

/**
 * Attempt to find a Dataset by mapping the id to a FDML descriptor.
 */
class FdmlDatasetResolver extends DatasetResolver {
  
  /**
   * Optionally return a Dataset with the given identifier if an FDML
   * definition can be found.
   * This method is used by the DatasetResolver ServiceLoader to determine
   * if this can provide the requested Dataset, so failures (None) are
   * to be expected.
   */
  def getDataset(id: String): Option[Dataset] =
    FdmlUtils.resolveFdmlUri(id + ".fdml").map(FdmlReader(_).getDataset)

}
