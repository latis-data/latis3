package latis.ops

import latis.data.SampledFunction
import latis.model.DataType

/**
 * Defines an Operation that acts on a single Dataset.
 */
trait UnaryOperation extends Operation {

  /**
   * Provides a new model resulting from this Operations
   * Default to no-op.
   */
  def applyToModel(model: DataType): DataType = model

  /**
   * Provides new Data resulting from this Operation.
   * Note that we request the model here and not just the input Data
   * since data operations often need the metadata/model.
   * Default to no-op.
   */
  def applyToData(data: SampledFunction, model: DataType): SampledFunction = data

}
