package latis.model

import latis.data._
import latis.metadata._

/**
 * A Dataset is a container of data and its metadata.
 */
case class Dataset(metadata: Metadata, model: DataType, data: Data) extends MetadataLike {
  
  /**
   * Return the data as an Iterator of Samples.
   * If the data is not a Function, implicitly 
   * make a Sample with an empty domain (arity = 0).
   * The model DataType must be consistent with the Data.
   */
  def samples: Iterator[Sample] = data match {
    case SampledFunction(it) => it
    case d: Data => Iterable(Sample(d)).iterator //implicit Sample
  }
  
  /**
   * Present a Dataset as a String.
   * This will only show type information and will not impact
   * the Data (e.g. no data reading should occur).
   */
  override def toString: String =  s"${apply("id")}: $model"
  
}
