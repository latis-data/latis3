package latis.util

import scala.collection._
import latis.model.Dataset

/**
 * A Singleton to hold instances of a Dataset in a Map with
 * the dataset identifier as the key.
 */
private class CacheManager {
  /*
   * TODO: extends DatasetSourceProvider, but as singleton?
   * resulting DatasetSource?
   *   getDataset(ops)? 
   *   could cache same dataset with ops applied?
   *   or simple apply them
   *   anonymous wrapper around getDatasetSource(id)?
   *   or cache entry as DatasetSource?
   * Or should we bypass DatasetSource and give user Dataset (akin to URI resolver => Stream)
   *   do we have a good reason to expose DatasetSource?
   *     no longer need to manage resources, e.g. close (in theory)
   *     easier to merge later that to break apart
   */
  
  /**
   * Maintain Datasets in a Map using the identifier as the key.
   * TODO: include the time cached so we can invalidate.
   */
  private val cache = mutable.Map[String, Dataset]()
  
  //TODO: concurrency issues, serialize methods?
}


/**
 * Companion object where we encapsulate the single instance and
 * expose the public methods.
 */
object CacheManager {
  
  /**
   * Singleton instance of the CacheManager.
   */
  private lazy val instance = new CacheManager()
  
  /**
   * Add the given dataset to the cache.
   */
  def cacheDataset(dataset: Dataset): Unit =
    instance.cache += dataset.id -> dataset
    //TODO: warn if data not memoized?
  
  /**
   * Optionally get the Dataset with the given name.
   */
  def getDataset(name: String): Option[Dataset] = {
    instance.cache.get(name)
  }
  
  /**
   * Return an immutable Map of dataset name to Dataset instance.
   */
  def getDatasets: immutable.Map[String, Dataset] = instance.cache.toMap  //make immutable

  /**
   * Remove all entries from the cache.
   */
  def clear: Unit = instance.cache.clear
  
  /**
   * Remove a single dataset from the cache.
   */
  def removeDataset(name: String): Option[Dataset] = instance.cache.remove(name)
  
  //TODO: validate: remove expired datasets? need md term for expiration
}
