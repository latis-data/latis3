package latis.ops

import cats.syntax.all.*

import latis.data.Data
import latis.data.StreamFunction
import latis.model.DataType
import latis.util.LatisException

/**
 * Joins two Datasets by appending their Streams of Samples.
 */
case class Append() extends Join {
  //TODO: assert that models are the same
  //TODO: deal with non-Function Data: add index domain? error?

  def applyToModel(
    model1: DataType,
    model2: DataType
  ): Either[LatisException, DataType] =
    model1.asRight

  def applyToData(data1: Data, data2: Data): Either[LatisException, Data] =
    StreamFunction(data1.samples ++ data2.samples).asRight

}
