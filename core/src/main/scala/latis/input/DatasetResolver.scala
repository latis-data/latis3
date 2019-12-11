package latis.input

import java.util.ServiceLoader

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.util.Properties
import scala.util.Try

import latis.dataset.Dataset
import latis.util.CacheManager
import latis.util.FileUtils
import latis.util.LatisConfig

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
   * return None. A failure to read the data could result in an empty Dataset
   * as opposed to None.
   */
  def getDataset(id: String): Option[Dataset]
  //TODO: "resolve"?

}

object DatasetResolver {

  /**
   * Finds a Dataset with the given identifier.
   *
   * If the identified Dataset does not exist in the cache,
   * this will find the DatasetResolver that can provide it.
   * If the Dataset is not found among any resolver throw a
   * RuntimeException.
   */
  def getDataset(id: String): Dataset =
    //TODO: return Option, Either?
    // could throw ServiceConfigurationError
    CacheManager.getDataset(id).getOrElse {
      ServiceLoader
        .load(classOf[DatasetResolver])
        .asScala
        .flatMap(_.getDataset(id))
        .headOption
        .getOrElse {
          throw new RuntimeException(s"Failed to resolve dataset: $id")
        }
    }

  /**
   * Find all datasets and return their IDs.
   */
  def getDatasetIds: Try[Seq[String]] = {
    //TODO: ask all DatasetResolvers to provide a list/catalog?
    val suffix = ".fdml"
    val searchPathFromConfig =
      LatisConfig.getOrElse("latis.fdml.dir", Properties.userDir)
    val searchPath = FileUtils
      .resolvePath(searchPathFromConfig)
      .toRight(new RuntimeException(s"File not found: $searchPathFromConfig"))
      .toTry

    searchPath.flatMap {
      FileUtils.getFileList(_, suffix).map {
        _.map(_.getFileName.toString.stripSuffix(suffix))
      }
    }
  }
}
