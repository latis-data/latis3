package latis.input.fdml

import java.net.URI
import java.nio.file.Paths

import cats.syntax.all._

import latis.dataset.Dataset
import latis.input.DatasetResolver
import latis.util._

/**
 * Attempt to find a Dataset by mapping the id to a FDML descriptor.
 */
class FdmlDatasetResolver extends DatasetResolver {

  /**
   * Optionally returns a Dataset with the given identifier if an FDML
   * definition can be found.
   * This method is used by the DatasetResolver ServiceLoader to determine
   * if this can provide the requested Dataset, so failures (None) are
   * to be expected.
   */
  def getDataset(id: String): Option[Dataset] = {
    val validate: Boolean = LatisConfig.getOrElse("latis.fdml.validate", false)
    val ds = for {
      uri     <- NetUtils.parseUri(id + ".fdml")
      fdml    <- resolveFdml(uri)
      dataset <- Either.catchNonFatal {
        FdmlReader.read(fdml, validate)
      }
    } yield dataset
    //TODO: need a way to capture Dataset construction failure
    // Any failure constructing a Dataset (not including reading data)
    // will result in None here which will manifext itself as a
    // ServiceProvider not found.
    ds.toOption
  }

  /**
   * Returns a fully resolved URI for an FDML file given a URI
   * that may be relative to the directory specified by the
   * latis.fdml.dir configuration option.
   */
  def resolveFdml(uri: URI): Either[LatisException, URI] =
    if (uri.isAbsolute) Either.right(uri)
    else {
      val dir = LatisConfig.getOrElse("latis.fdml.dir", "datasets")
      //TODO: look in home? $LATIS_HOME?, classpath?
      NetUtils.resolveUri(Paths.get(dir, uri.getPath).toString)
      // Note on use of toString:
      // Path.toUri prepends the home directory as the base URI
    }

}
