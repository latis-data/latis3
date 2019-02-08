package latis.input

import latis.model.Dataset
import latis.ops.Operation
import latis.ops.UnaryOperation
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import java.util.ServiceLoader


/**
 * A DatasetSource is a provider of a LaTiS Dataset
 * as opposed to an Adapter that just provides data.
 * Provided Operations should be applied lazily to the Dataset.
 */
trait DatasetSource {
  
  /**
   * Return the Dataset with the given Operations applied to it.
   */
  def getDataset(operations: Seq[UnaryOperation]): Dataset
  
  /**
   * Return the Dataset with no Operations applied to it.
   */
  def getDataset(): Dataset = getDataset(Seq.empty)
  
}

object DatasetSource {
  
  /**
   * Get the DatasetSource for a given dataset by its identifier.
   * Delegate to known DatasetSourceProviders.
   */
  def fromName(datasetId: String): DatasetSource =
    ServiceLoader.load(classOf[DatasetSourceProvider]).asScala
      .flatMap(_.getDatasetSource(datasetId)).headOption
      .getOrElse(throw new RuntimeException(s"Failed to find source for dataset: $datasetId"))
      
//TODO: try CacheManager first
//TODO: DatasetSourceProvider to Look for a matching "reader" property.
//    LatisProperties.get(s"reader.${datasetId}.class") match {
//      case Some(s) =>
//        Class.forName(s).getConstructor().newInstance().asInstanceOf[DatasetSource]
//    }
}

trait DatasetSourceProvider {
  def getDatasetSource(dsid: String): Option[DatasetSource]
}




