package latis.input

import latis.data.SampledFunction
import latis.metadata.Metadata
import latis.model.DataType
import latis.dataset._
import latis.ops._

import java.net.URI

/**
 * DatasetReader that uses an Adapter to get data from the source.
 */
trait AdaptedDatasetReader extends DatasetReader {
  
  /**
   * A Uniform Resource Identifier (URI) of the data source
   * from which the Adapter can read data.
   */
  def uri: URI
  
  /**
   * Global metadata.
   * Use the path of the URI as the Dataset identifier by default.
   */
  def metadata: Metadata = Metadata("id" -> uri.getPath)
  
  /**
   * Data model for the Dataset.
   */
  def model: DataType
  
  /**
   * Adapter to provide data from the data source.
   */
  def adapter: Adapter
  
  /**
   * Predefined Operations to be applied to the Dataset.
   * Default to none.
   */
  def operations: Seq[UnaryOperation] = Seq.empty
  
  /**
   * Construct a Dataset by delegating to the Adapter.
   */
  def getDataset: Dataset = {
    new AdaptedDataset(
      metadata,
      model,
      adapter,
      uri,
      operations
    )
    
//    // Apply the Adapter to the given resource to get the data.
//    val data: SampledFunction = adapter(uri)
//    
//    // Construct the Dataset so far.
//    val dataset = Dataset(metadata, model, data)
//    
//    // Apply the operations to the Dataset.
//    // Note that some operation may be handled by the SampledFunction
//    // that was provided by a "smart" Adapter.
//    operations.foldLeft(dataset)((ds, op) => op(ds))
  }
}
