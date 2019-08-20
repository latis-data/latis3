package latis.ops

import latis.data._
import latis.model._

/**
 * The GroupByBin GroupingOperation will use the given domainSet
 * to provide the new DomainData values. Each Sample in the 
 * incoming Dataset will be placed into the DomainSet bin that
 * it falls in. The given Aggregation will then reduce the Samples
 * in each bin into a single RangeData value (which could be a
 * nested Function).
 */
case class GroupByBin(
  domainSet: DomainSet, 
  aggregation: Aggregation = NoAggregation(),
  //csx: Dataset = null
) extends GroupingOperation {
  
//  /*
//   * Hack the CSX in for now to support MODIS
//   * The csx Dataset must have:
//   *   the domain of the incoming Dataset
//   *   the range matching the domainSet (thus Ordered)
//   * Note that model will take care of itself.
//   */
//  val csxf: DomainData => DomainData = if (csx != null) csx.data match {
//    case mf: MemoizedFunction => (dd: DomainData) =>
//      DomainData.fromSeq(mf(dd).get.map(_.asInstanceOf[OrderedData])) //TODO: clean up
//  } else identity

  /*
   * TODO: fill empty bins
   * applyToData then...
   *   join with fill dataset?
   *   union, keeping the first when present
   * seems OK since SF.gb wouldn't know what to fill with
   * 
   */
  /**
   * Add fill data to empty domainSet bins
   * after applying the grouping and aggregation.
   */
//  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
//    
//    val groupedData = super.applyToData(data, model)
//    
//    val fillData: RangeData = applyToModel(model) match {
//      case Function(_, range) => range.makeFillValues
//    }
//    
//    val range = domainSet.elements map { domain =>
//      groupedData(domain) getOrElse fillData
//    }
//    /*
//     * TODO: evaluating RDD via lookup is slow
//     * should we start the bins will fill data?
//     * or join after?
//     *   join keeps only same keys
//     *   union keeps all with duplicates, not clear if we can keep the one we want
//     * generic groupBy doesn't know the domain bins a priori
//     * making a full dataset of fill values seem unfortunate
//     * might lookup be ok if we had partitioner?
//     * 
//     * How can we inject a different Partitioner?
//     * e.g LinearSet1D 
//     * RDD specific, shouldn't be exposed here
//     * need higher level entry for groupByBin?
//     * or construct RDDFunction based on domain set of data
//     *   has to be memoized anyway, 
//     *   get first/last samples 
//     *   and count?
//     * then how does groupBy decide how to partition?
//     *   GBB could use domainSet but would be nice to be more generic
//     *   coverage or valid range in metadata?
//     */
//
//    SetFunction(domainSet, range)
//  }
  
  /**
   * Determine the new DomainData value for each Sample.
   * For GroupByBin, the domain value will be the domainSet 
   * bin that contains the Sample.
   */
  def groupByFunction: Sample => Option[DomainData] = (sample: Sample) => {
    val index = domainSet.indexOf(sample.domain)
    if (index >= 0 && index < domainSet.length) domainSet.getElement(index)
    else None  //invalid sample not within the domain set
  }
  
  /**
   * Override applyToModel to use the DomainSet's type for the domain
   * and delegate to the Aggregation to provide the new model for the
   * range.
   */
  override def applyToModel(model: DataType): DataType = {
    val range = model match {
      case Function(_, r) => aggregation.applyToModel(r)
    }
    Function(domainSet.model, range)
  } 
}
