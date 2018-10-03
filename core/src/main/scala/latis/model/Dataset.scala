package latis.model

import latis.data.Sample
import latis.data.SampledFunction
import latis.metadata.Metadata
import latis.metadata.MetadataLike

import cats.effect.IO
import fs2.Stream

/**
 * A Dataset is the primary representation of any dataset.
 * It contains global metadata (including provenance), 
 * a representation of the dataset's model (or schema), 
 * and a SampledFunction that encapsulates the data.
 */
case class Dataset(metadata: Metadata, model: DataType, data: SampledFunction)
  extends MetadataLike {
  
  /**
   * Return the data as an effectful Stream of Samples.
   * The model DataType must be consistent with the data.
   */
  def samples: Stream[IO, Sample] = data.samples
  
  /**
   * Present a Dataset as a String.
   * This will only show type information and will not impact
   * the Data (e.g. lazy data reading should not be triggered).
   */
  override def toString: String =  s"${this("id")}: $model"
  
}
