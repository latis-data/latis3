package latis.ops

import latis.data._
import latis.model._

/**
 * Defines a GroupOperation that uses the given domainSet
 * to provide new DomainData values. Each Sample in the
 * incoming Dataset will be placed into the DomainSet bin that
 * it overlaps. The given Aggregation will then reduce the Samples
 * in each bin into a single RangeData value (which could be a
 * nested Function).
 */
case class GroupByBin(
  domainSet: DomainSet
) extends GroupOperation {

  def aggregation: Aggregation = DefaultAggregation()

  def domainType(model: DataType): DataType = domainSet.model

  /**
   * Determines the new DomainData value for each Sample.
   * For GroupByBin, the domain value will be the domainSet
   * bin that contains the Sample.
   */
  def groupByFunction(model: DataType): Sample => Option[DomainData] =
    (sample: Sample) => {
      val index = domainSet.indexOf(sample.domain)
      if (index >= 0 && index < domainSet.length) domainSet(index)
      else None //invalid sample not within the domain set
    }
}
