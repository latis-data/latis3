package latis.ops

import latis.data.Data
import latis.model.DataType
import latis.util.LatisException

/**
 * Defines an Operation that combines two Datasets into one.
 */
trait BinaryOperation extends Operation {
  /*
  TODO: consider dropping BinaryOperations for UnaryOperations
   that take the initial dataset as an argument.
   Then we could drop "Unary"
   Would this give up opportunities to fold a collection of datasets?
   */

  /**
   * Combines the models of two Datasets.
   */
  def applyToModel(model1: DataType, model2: DataType): Either[LatisException, DataType]

  /**
   * Combines the Data of two Datasets.
   */
  def applyToData(data1: Data, data2: Data): Either[LatisException, Data]

}
