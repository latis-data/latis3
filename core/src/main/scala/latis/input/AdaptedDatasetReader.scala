package latis.input

import java.net.URI

import latis.dataset._
import latis.metadata.Metadata
import latis.model.DataType
import latis.ops._

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
  def getDataset: Dataset =
    new AdaptedDataset(
      metadata,
      model,
      adapter,
      uri,
      operations
    )

}
