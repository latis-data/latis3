package latis.input

import java.net.URI

import latis.dataset._
import latis.metadata.Metadata
import latis.model.DataType
import latis.ops._

/**
 * Defines a DatasetReader that uses an Adapter to get data from the source.
 */
trait AdaptedDatasetReader extends DatasetReader {

  /** Returns global metadata. */
  def metadata: Metadata

  /** Returns the data model of the Dataset. */
  def model: DataType

  /**
   * Returns the Adapter that provides data from the data source.
   */
  def adapter: Adapter

  /**
   * Defines Operations to be applied to the Dataset.
   * Defaults to none.
   */
  def operations: List[UnaryOperation] = List.empty

  /**
   * Returns a potentially lazy Dataset represented by the given URI.
   */
  def read(uri: URI): Dataset =
    new AdaptedDataset(
      metadata,
      model,
      adapter,
      uri,
      operations
    )

}
