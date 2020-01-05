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

  def applyToData(
    data1: SampledFunction,
    data2: SampledFunction
  ): SampledFunction =
    SampledFunction(data1.samples ++ data2.samples)

}
