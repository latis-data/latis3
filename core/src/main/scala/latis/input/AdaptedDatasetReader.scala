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
  /*
  TODO: not suitable for DatasetReader ServiceLoader
simply tries to make a Dataset and may succeed even if this isn't the best provider
Is there any value for DatasetReader?
  supports Dataset.fromUri(uri)
  e.g. based on file suffix
Do we need better separation between saying yes and doing?
  maybe not, could be used to find backup provider
Don't want eager providers messing things up
  that could happen either way
extend DatasetReaderProvider?
  then return Option[DatasetReader]
  in class
  the rest of the logic could be in the companion object, even extending AdaptedDatasetReader
  then HysicsImageReaderOperation could get its model
   */

//  /**
//   * A Uniform Resource Identifier (URI) of the data source
//   * from which the Adapter can read data.
//   */
//  def uri: URI

  /**
   * Global metadata.
   * Use the path of the URI as the Dataset identifier by default.
   */
  def metadata: Metadata // = Metadata("id" -> uri.getPath)

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

  override def read(uri: URI): Option[Dataset] = {
    Some(
      new AdaptedDataset(
        metadata,
        model,
        adapter,
        uri,
        operations
      )
    )
  }

//  /**
//   * Construct a Dataset by delegating to the Adapter.
//   */
//  def getDataset: Dataset =
//    new AdaptedDataset(
//      metadata,
//      model,
//      adapter,
//      uri,
//      operations
//    )

}
