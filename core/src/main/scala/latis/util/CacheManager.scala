package latis.util

import scala.collection._
import latis.model.Dataset
import latis.input.DatasetSource

/**
 * Manage a cache to hold instances of a Dataset in a Map with
 * the dataset identifier as the key.
 */
class CacheManager extends DatasetSource {

  /**
   * This method is used by the DatasetSource ServiceLoader to determine
   * if this can provide the requested Dataset.
   */
  def getDataset(name: String): Option[Dataset] = CacheManager.cache.get(name)

}


/**
 * Companion object where we encapsulate the single instance and
 * expose the public methods.
 */
object CacheManager {
  
  /**
   * Singleton instance of the CacheManager.
   */
  //TODO: concurrency issues, serialize methods? Use scalacache!
  private lazy val cache = mutable.Map[String, Dataset]()
  
  /**
   * Add the given Dataset to the cache.
   * This will also ensure that the Dataset is memoized
   * via a potentially unsafe read.
   */
  def cacheDataset(dataset: Dataset): Unit = 
    cache += dataset.id -> dataset.unsafeForce
  
  /**
   * Optionally get the Dataset with the given name.
   */
  def getDataset(name: String): Option[Dataset] = cache.get(name)
  
  /**
   * Return an immutable Map of dataset name to Dataset instance.
   */
  def getDatasets: immutable.Map[String, Dataset] = cache.toMap  //make immutable

  /**
   * Remove all entries from the cache.
   */
  def clear: Unit = cache.clear
  
  /**
   * Remove a single dataset from the cache.
   */
  def removeDataset(name: String): Option[Dataset] = cache.remove(name)
  
  //TODO: validate: remove expired datasets? need md term for expiration
}

