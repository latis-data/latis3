package latis.ops

import latis.data._
import latis.model.DataType

/**
 * Joins two Datasets by appending their Streams of Samples.
 */
case class Append() extends BinaryOperation {
  //TODO: assert that models are the same
  //TODO: deal with ConstantFunctions, add index domain
  //TODO: consider CompositeSampledFunction

  def applyToModel(model1: DataType, model2: DataType): DataType = model1

  def applyToData(data1: Data, data2: Data): SampledFunction =
    SampledFunction(data1.asFunction.samples ++ data2.asFunction.samples)

}
