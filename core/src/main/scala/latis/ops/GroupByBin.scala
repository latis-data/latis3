package latis.ops

import scala.collection.immutable.SortedMap

import cats.data.Chain

import latis.data.*
import latis.model.*
import latis.util.LatisException

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

  /**
   * Extends the default by constructing a SetFunction with the domainSet.
   */
  override def applyToData(data: Data, model: DataType): Either[LatisException, Data] =
    for {
      data <- super.applyToData(data, model)
      range = data.asInstanceOf[SampledFunction].unsafeForce.sampleSeq.map(_.range).toIndexedSeq
    } yield SetFunction(domainSet, range)

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

  /*
  TODO: make sure all bins have data or fill
    currently done by making all empty bin
    then use Agg to fill empty bins
    doesn't work for spark
    can we fill here after the GB?
    compose?
    what would it look like in spark
      join
   */

  /**
   * Override the default making of the temporary SortedMap
   * to start with an entry for each element of the given DomainSet.
   * Otherwise, there would be no entry for empty bins.
   */
  override def makeSortedMap(model: DataType): SortedMap[DomainData, Chain[Sample]] = {
    val pairs = domainSet.elements.toList.map(dd => (dd, Chain.empty))
    SortedMap.from(pairs)(ordering(model))
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
