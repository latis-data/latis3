package latis.ops

import cats.syntax.all._

import latis.data.Data
import latis.data.StreamFunction
import latis.model.DataType
import latis.util.LatisException

/**
 * Joins two Datasets by appending their Streams of Samples.
 */
case class Append() extends BinaryOperation {
  //TODO: assert that models are the same
  //TODO: deal with ConstantFunctions, add index domain
  //TODO: consider CompositeSampledFunction

  def applyToModel(
    model1: DataType,
    model2: DataType
  ): Either[LatisException, DataType] =
    model1.asRight

  def applyToData(data1: Data, data2: Data): Either[LatisException, Data] =
    StreamFunction(data1.asFunction.samples ++ data2.asFunction.samples).asRight

}
