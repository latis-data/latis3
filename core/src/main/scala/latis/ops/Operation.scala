package latis.ops

import latis.model.Dataset

/**
 * Placeholder until we migrate Operations.
 */
trait Operation {
  
  def apply(dataset: Dataset): Dataset
  
}