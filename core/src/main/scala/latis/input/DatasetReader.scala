package latis.input

import latis.dataset.Dataset
import java.net.URI
import java.util.ServiceLoader
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
 * DatasetReader is a trait to be extended by a class that can take a URI
 * and attempt to construct a Dataset from it. If a DatasetReader subclass
 * can't read that URL (based on the type of the data source, not due to an
 * IO error), then it should return None.
 * Provide a default read that returns None so subclasses can chose whether
 * to override it (along with an entry in META-INF/services).
 */
trait DatasetReader {
  def read(uri: URI): Option[Dataset] = None
}

object DatasetReader {
  
  def read(uri: URI): Dataset =
    ServiceLoader.load(classOf[DatasetReader]).asScala
      .flatMap(_.read(uri)).headOption.getOrElse {
        val msg = s"Failed to find a DatasetReader for URI: uri"
        throw new RuntimeException(msg)
        //TODO: return empty Dataset?  Dataset[IO]?
      }
  
}
