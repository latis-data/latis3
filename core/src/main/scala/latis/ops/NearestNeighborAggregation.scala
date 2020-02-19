package latis.ops

import latis.data._
import latis.model._

/**
 * Defines an Aggregation that reduces the Samples of a Dataset to a ConstantFunction
 * with the range of the single Sample whose domain is closest to the given domain.
 */
case class NearestNeighborAggregation(domain: DomainData) extends Aggregation {

  def aggregateFunction(model: DataType): Iterable[Sample] => Data = {
    (samples: Iterable[Sample]) =>
      if (samples.isEmpty) model match {
        case Function(_, r) => r.fillValue
        case _ => ??? //TODO: error, model must be a Function
      } else {
        val sample = samples.minBy {
          case Sample(dd, _) => DomainData.distance(domain, dd)
        }
        Data.fromSeq(sample.range)
      }
  }

  def applyToModel(model:DataType): DataType = model match {
    case Function(_, r)=> r
    case _ => ??? //TODO: error, model must be a Function
  }
}
