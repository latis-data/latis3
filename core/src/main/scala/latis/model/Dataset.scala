package latis.model

import latis.data.FunctionFactory
import latis.data.SampledFunction
import latis.input.DatasetReader
import latis.input.DatasetResolver
import latis.metadata.Metadata
import latis.metadata.MetadataLike
import latis.util.CacheManager

import java.net.URI

/**
 * A Dataset is the primary representation of any dataset.
 * It contains global metadata (including provenance), 
 * a representation of the dataset's model (or schema), 
 * and a SampledFunction that encapsulates the data.
 */
case class Dataset(metadata: Metadata, model: DataType, data: SampledFunction)
  extends MetadataLike {
  //TODO: impl FunctionalAlgebra by delegating to Operations? DatasetOps?
  //TODO: since data is always a SampledFunction, should model always be a Function?

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
  def restructure(ff: FunctionFactory): Dataset =
    copy(data = ff.restructure(data))
    
  // Rename the dataset by making a new copy with updated metadata.
  //TODO: impl as an Operation, capture prov
  def rename(name: String): Dataset =
    copy(metadata = metadata + ("id" -> name))
  
  /**
   * Ensure that the data encapsulated by this Dataset is memoized.
   * It is "unsafe" in that this may perform an unsafe run on a Stream
   * of Samples in IO.
   */
  def unsafeForce: Dataset = copy(data = data.unsafeForce)
  
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
