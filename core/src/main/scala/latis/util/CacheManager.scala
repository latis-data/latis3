package latis.util

import scala.collection._
import latis.dataset.{Dataset, MemoizedDataset}

/**
 * Companion object where we encapsulate the single instance and
 * expose the public methods.
 */
object CacheManager {
  //TODO: concurrency issues, serialize methods? Use scalacache!
  //TODO: validate: remove expired datasets? need md term for expiration
  //TODO: replace String IDs with Identifiers?

  /**
   * Singleton instance of the CacheManager.
   */
  private lazy val cache = mutable.Map[String, Dataset]()

  /**
   * Add the given Dataset to the cache.
   * This will also ensure that the Dataset is memoized
   * via a potentially unsafe read.
   */
  def cacheDataset(dataset: MemoizedDataset): Unit = {
    val dsId = dataset.id match {
      case Some(id) => id.asString
      case None => ""
    }
    cache += dsId -> dataset
  }

  /**
   * Optionally get the Dataset with the given id.
   */
  def getDataset(id: String): Option[Dataset] = cache.get(id)

  /**
   * Return an immutable Map of dataset id to Dataset instance.
   */
  def getDatasets: immutable.Map[String, Dataset] = cache.toMap //make immutable

  /**
   * Remove all entries from the cache.
   */
  def clear(): Unit = cache.clear()

  /**
   * Remove a single dataset from the cache.
   */
  def removeDataset(id: String): Option[Dataset] = cache.remove(id)

}
