package latis.input

import latis.data.SampledFunction
import latis.metadata.Metadata
import latis.model.DataType
import latis.model.Dataset
import latis.ops._

import java.net.URI

/**
 * DatasetReader that uses an Adapter to get data from the source.
 */
trait AdaptedDatasetReader extends DatasetReader {
  
  /**
   * Resolvable identifier of the data source.
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
  //def getDataset(ops: Seq[UnaryOperation]): Dataset = {
  def getDataset: Dataset = {
    
    // Apply the Adapter to the given resource to get the data.
    val data: SampledFunction = adapter(uri)
    
    // Construct the Dataset so far.
    val dataset = Dataset(metadata, model, data)
    
    // Apply the operations to the Dataset.
    // Note that the operations from this DatasetSource will be applied first.
    //TODO: allow Adapter to apply operations
    //(operations ++ ops).foldLeft(dataset)((ds, op) => op(ds))
    operations.foldLeft(dataset)((ds, op) => op(ds))
  }
}
