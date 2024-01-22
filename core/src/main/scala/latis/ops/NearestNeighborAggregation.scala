package latis.ops

import latis.data.*
import latis.model.*
import latis.util.LatisException

/**
 * Defines an Aggregation that reduces the Samples of a Dataset
 * to a single zero-arity Sample with the range of the single Sample
 * whose domain is closest to the given domain.
 */
case class NearestNeighborAggregation(domain: DomainData) extends Aggregation {

  def aggregateFunction(model: DataType): Iterable[Sample] => Data = {
    (samples: Iterable[Sample]) =>
      if (samples.isEmpty) model match {
        case Function(_, r) => r.fillData
        case _ => ??? //TODO: error, model must be a Function
      } else {
        val sample = samples.minBy { sample =>
          DomainData.distance(domain, sample.domain)
        }
        Data.fromSeq(sample.range)
      }
  }

  def applyToModel(model:DataType): Either[LatisException, DataType] = model match {
    case Function(_, r) => Right(r)
    case _ => Left(LatisException("Model must be a Function"))
  }
}
