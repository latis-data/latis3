package latis.ops

import latis.data._
import latis.model._

/**
 * Defines an Aggregation that reduces the Samples of a Dataset to a ConstantFunction
 * with the range of the first Sample.
 */
case class HeadAggregation() extends Aggregation {

  def aggregateFunction(model: DataType): Iterable[Sample] => RangeData =
    (samples: Iterable[Sample]) =>
      if (samples.isEmpty) applyToModel(model) match {
        case f: Function => RangeData(SeqFunction(Seq.empty)) //can't fill Function, use empty
        case dt => dt.fillValue
      }
      else samples.head.range

  def applyToModel(model:DataType): DataType = model match {
    case Function(_, r)=> r
    case _ => ??? //TODO: error, model must be a Function
  }
}
