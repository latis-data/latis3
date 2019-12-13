package latis.ops

import latis.data.Data
import latis.model.DataType

/**
 * Defines an Operation that combines two Datasets into one.
 */
trait BinaryOperation extends Operation {

  /**
   * Combines the models of two Datasets.
   */
  def applyToModel(model1: DataType, model2: DataType): DataType

  /**
   * Combines the Data of two Datasets.
   */
  def applyToData(
    model1: DataType,
    data1: Data,
    model2: DataType,
    data2: Data
  ): Data

}
