package latis.ops

import latis.data.Data
import latis.model.DataType
import latis.util.LatisException

/**
 * Defines an Operation that acts on a single Dataset.
 */
trait UnaryOperation extends Operation {

  /**
   * Provides a new model resulting from this Operation.
   */
  def applyToModel(model: DataType): Either[LatisException, DataType]

  /**
   * Provides new Data resulting from this Operation.
   */
  def applyToData(data: Data, model: DataType): Either[LatisException, Data]

}
