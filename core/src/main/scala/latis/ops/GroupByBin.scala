package latis.ops

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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
  domainSet: DomainSet,
  aggregation: Aggregation = DefaultAggregation()
) extends GroupOperation {

  /*
  TODO: NearestNeaighborAgg, need diff agg for each bin, with DomainData to be closest to
    AggProvider? takes a DomainData
      is that applicable to other aggs?
      seems unique to NN
      may need special GBBNN
      use of agg is in GroupOp.pipe, gets aggFunction
      optional arg to aggregation.aggregateFunction?
    solution needs to work for spark, also
      we use mapValues to agg with RDD
      NNAgg would need map
      can't hack together because we are using RDD.groupBy
        which must finish before we can agg
   */

  /**
   * Override the default making of the temporary SortedMap
   * to start with an entry for each element of the given DomainSet.
   * Otherwise, there would be no entry for empty bins.
   */
  override def makeSortedMap(model: DataType): mutable.SortedMap[DomainData, ListBuffer[Sample]] = {
    val smap = super.makeSortedMap(model)
    domainSet.elements.foreach { dd =>
      smap += (dd -> ListBuffer[Sample]())
    }
    smap
  }

  /**
   * Gets the new domain type from the DomainSet.
   */
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
