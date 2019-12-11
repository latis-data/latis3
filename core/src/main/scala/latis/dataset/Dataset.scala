package latis.dataset

import java.net.URI

import cats.effect.IO
import fs2.Stream
import latis.data.{FunctionFactory, Sample}
import latis.input.DatasetReader
import latis.input.DatasetResolver
import latis.metadata.Metadata
import latis.metadata.MetadataLike
import latis.model.DataType
import latis.ops.UnaryOperation
import latis.util.CacheManager

/**
 * Defines the interface for a LaTiS Dataset.
 */
trait Dataset extends MetadataLike {

  /**
   * Returns the Metadata describing this Dataset.
   */
  def metadata: Metadata

  /**
   * Returns the DataType describing the structure of this Dataset
   * including metadata for each of its variables.
   */
  def model: DataType

  /**
   * Returns a lazy fs2.Stream of Samples.
   */
  def samples: Stream[IO, Sample]

  /**
   * Returns a new Dataset with the given Operation *logically*
   * applied to this one.
   */
  def withOperation(op: UnaryOperation): Dataset

  /**
   * Returns a new Dataset with the given Operations *logically*
   * applied to this one.
   */
  def withOperations(ops: Seq[UnaryOperation]): Dataset =
    ops.foldLeft(this)((ds, op) => ds.withOperation(op))

  /**
   * Causes the data source to be read and released
   * and existing Operations to be applied.
   */
  def unsafeForce(): MemoizedDataset

  /**
   * Puts a copy of this Dataset into the CacheManager.
   * Note that the data will be memoized, triggering
   * data reading and operation application.
   */
  def cache(): Unit = CacheManager.cacheDataset(unsafeForce())

  //use FunctionFactory arg to restructure SF to something optimal
  def restructureWith(ff: FunctionFactory): MemoizedDataset = {
    val ds = this.unsafeForce()
    new MemoizedDataset(
      ds.metadata,
      ds.model,
      ff.restructure(ds.data)
    )
  }

  /**
   * Returns a String representation of this Dataset.
   * This will only show type information and will not impact
   * the Data (e.g. lazy data reading should not be triggered).
   */
  override def toString: String = s"$id: $model"
}

object Dataset {

  /**
   * Creates a Dataset by using the DatasetResolver ServiceLoader.
   */
  def fromName(name: String): Dataset = DatasetResolver.getDataset(name)

  /**
   * Creates a Dataset by using the DatasetReader ServiceLoader.
   */
  def fromURI(uri: URI): Dataset = DatasetReader.read(uri)

}
