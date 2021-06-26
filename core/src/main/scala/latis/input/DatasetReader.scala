package latis.input

import java.net.URI
import java.util.ServiceLoader

import scala.jdk.CollectionConverters._

import latis.dataset.Dataset
import latis.model.DataType
import latis.util.LatisException

/**
 * Defines a trait to be extended by a class that can take a URI
 * and attempt to construct a Dataset from it. If a DatasetReader subclass
 * can't read that URL (based on the type of the data source, not due to an
 * IO error), then canRead should return false. This is the default response
 * so it only needs to be overridden by readers that are registered to use
 * the ServiceProvider mechanism (with an entry in META-INF/services).
 */
trait DatasetReader {
  def model: DataType
  @annotation.nowarn("msg=never used")
  def canRead(uri: URI): Boolean = false
  def read(uri: URI): Dataset
}

object DatasetReader {

  def read(uri: URI): Dataset =
    ServiceLoader
      .load(classOf[DatasetReader])
      .asScala
      .find(_.canRead(uri))
      .map(_.read(uri))
      .getOrElse {
        val msg = s"Failed to find a DatasetReader for: uri"
        throw LatisException(msg)
      }

}
