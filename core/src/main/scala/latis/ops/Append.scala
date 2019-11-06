package latis.ops

import latis.data._
import latis.dataset.Dataset
import latis.metadata.Metadata
import latis.model.DataType
import fs2.Stream
import cats.effect.IO

/**
 * Join two Datasets by appending their Streams of Samples.
 */
case class Append() extends BinaryOperation {
  //TODO: assert that models are the same
  //TODO: consider CompositeSampledFunction
  
  def applyToModel(model1: DataType, model2: DataType): DataType = model1
  
  def applyToData(
    model1: DataType,
    data1: SampledFunction,
    model2: DataType,
    data2: SampledFunction
  ): SampledFunction = {
    SampledFunction(data1.streamSamples ++ data2.streamSamples)
  }

}
