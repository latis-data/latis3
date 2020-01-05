package latis.ops

import latis.data.SampledFunction
import latis.model.DataType

/**
 * Defines an Operation that acts on a single Dataset.
 */
trait UnaryOperation extends Operation {

  /**
   * Provides a new model resulting from this Operation.
   */
  def applyToModel(model: DataType): DataType

  /**
   * Provides new Data resulting from this Operation.
   */
  def applyToData(data: SampledFunction, model: DataType): SampledFunction

}
