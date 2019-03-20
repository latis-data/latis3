package latis.input

import latis.model.Dataset
import latis.ops.Operation
import latis.ops.UnaryOperation
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import java.util.ServiceLoader


/**
 * A DatasetResolver provides a LaTiS Dataset given an identifier.
 * This trait can be implemented by any class that can resolve a Dataset.
 * Such classes registered in META-INF/services/ will be used by the 
 * ServiceLoader to attempt to get the desired Dataset.
 */
trait DatasetResolver {
  
  /**
   * Optionally return the Dataset with the given identifier.
   * If the DatasetResolver implementation does not support the given Dataset
   * return Null. A failure to read the data could result in an empty Dataset
   * as opposed to None.
   */
  def getDataset(id: String): Option[Dataset]

}


object DatasetResolver {
  
  /**
   * Find the DatasetResolver that can provide the requested dataset by name
   * among the DatasetResolver registered in META-INF/services/ of jar files 
   * in the classpath.
   * If the Dataset is not found among any resolver throw a RuntimeException.
   */
  def getDataset(id: String): Dataset = {
    ServiceLoader.load(classOf[DatasetResolver]).asScala
      .flatMap(_.getDataset(id)).headOption.getOrElse {
        throw new RuntimeException(s"Failed to resolve dataset: $id")
      }
  }
  
}
