package latis.ops

import latis.model.Dataset
import latis.data.StreamFunction

/**
 * Join two Datasets by appending their Streams of Samples.
 */
case class Append() extends BinaryOperation {
  
  def apply(ds1: Dataset, ds2: Dataset): Dataset = (ds1, ds2) match {
    case (Dataset(md1, model1, data1),Dataset(md2, model2, data2)) =>
      //TODO: deal with metadata, prov
      //TODO: assert that model1 = model2
      val data = StreamFunction(data1.streamSamples ++ data1.streamSamples)
      Dataset(md1, model1, data)
  }
}