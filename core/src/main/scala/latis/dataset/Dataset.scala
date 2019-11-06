package latis.dataset

import latis.model.DataType
import latis.metadata.Metadata
import latis.util.StreamUtils._
import fs2.Stream
import latis.data._
import cats.effect.IO
import latis.ops.UnaryOperation
import latis.input.Adapter
import java.net.URI
import latis.metadata.MetadataLike
import latis.input.DatasetResolver
import latis.input.DatasetReader
import latis.util.CacheManager

trait Dataset extends MetadataLike {
  
  def metadata: Metadata
  
  def model: DataType
  
  def samples: Stream[IO, Sample]
  
  def withOperation(op: UnaryOperation): Dataset
  //withOperations(operations: Seq[UnaryOperation]): Dataset?
  
  /**
   * Causes the data source to be read and releases
   * and existing Operations to be applied.
   */
  def unsafeForce: UnadaptedDataset = ???
  
  /**
   * Put a copy of this Dataset into the CacheManager.
   */
  def cache(): Unit = CacheManager.cacheDataset(this)
  
  /**
   * Make a copy of the Dataset with the data stored using
   * the given SampledFunction implementation.
   */
  /*
   * TODO: "cache", "persist", "memoize", ...?
   * use restructure to change the form of the data for (computation optimization?), could be lazy
   * use "cache" to force read, operations, and memoize data to release resources, unsafe
   */
  //def restructure(ff: FunctionFactory): Dataset = ??? //copy(data = ff.restructure(data))
    
  /**
   * Present a Dataset as a String.
   * This will only show type information and will not impact
   * the Data (e.g. lazy data reading should not be triggered).
   */
  override def toString: String =  s"${id}: $model"
} 


object Dataset {
    
  /**
   * Create a Dataset by using the DatasetResolver ServiceLoader.
   */
  def fromName(name: String): Dataset = DatasetResolver.getDataset(name)
  
  /**
   * Create a Dataset by using the DatasetReader ServiceLoader.
   */
  def fromURI(uri: URI): Dataset = DatasetReader.read(uri)

}
