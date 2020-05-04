package latis.input

import java.net.URI

import scala.xml.XML

import cats.implicits._

import latis.dataset.AdaptedDataset
import latis.dataset.Dataset
import latis.util.FdmlUtils
import latis.util.NetUtils

/**
 * From an FDML file an FdmlReader reader creates a dataset,
 * configures its adapter, and builds the dataset's model.
 */
final class FdmlReader(cl: ClassLoader) {

  def read(uri: URI, validate: Boolean = false): Dataset = {
    if (validate) FdmlUtils.validateFdml(uri).leftMap(throw _)
    val fdml: Fdml = NetUtils.readUriIntoString(uri) match {
      case Right(x) => new Fdml(XML.loadString(x), cl)
      case Left(e)  => throw e
    }
    new AdaptedDataset(
      fdml.metadata,
      fdml.model,
      fdml.adapter,
      fdml.uri
      //TODO: operations
    )
  }
}

object FdmlReader {

  /** Create an FdmlReader that uses the caller's class loader. */
  def apply(): FdmlReader = new FdmlReader(getClass().getClassLoader())
}
