package latis.model

import latis.data._
import latis.metadata._

/**
 * A Dataset is the primary representation of any dataset.
 * It contains global metadata (including provenance), 
 * a representation of the dataset's model (or schema), 
 * and a SampledFunction that provides the data.
 */
case class Dataset(metadata: Metadata, model: DataType, data: SampledFunction)
  extends MetadataLike {
  
  /**
   * Return the data as an Iterator of Samples.
   * The model DataType must be consistent with the Data.
   */
  def samples: Iterator[Sample] = data.samples
  
  /**
   * Present a Dataset as a String.
   * This will only show type information and will not impact
   * the Data (e.g. lazy data reading should not be triggered).
   */
  override def toString: String =  s"${this("id")}: $model"
  
}
