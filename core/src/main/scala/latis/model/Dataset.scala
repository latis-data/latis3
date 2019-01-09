package latis.model

import latis.data._
import latis.metadata._

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
  //TODO: impl FunctionalAlgebra by delegating to Operations?

  def cache(ff: FunctionFactory): Dataset = {
    //TODO: consider how "cache" relates to "force"; same?
    //TODO: generic empty vs specific type empty? zero for appending
    if (data.isEmpty) this
    else {
      val d2 = ff.fromSeq(data.unsafeForce.samples)
      copy(data = d2)
    }
  }
  
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
