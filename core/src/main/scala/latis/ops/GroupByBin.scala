package latis.ops

import latis.data._
import latis.model._

/**
 * The GroupByBin GroupingOperation will use the given domainSet
 * to provide the new DomainData values. Each Sample in the 
 * incoming Dataset will be placed into the DomainSet bin that
 * it overlaps. The given Aggregation will then reduce the Samples
 * in each bin into a single RangeData value (which could be a
 * nested Function).
 */
case class GroupByBin(
  domainSet: DomainSet, 
  aggregation: Aggregation = NoAggregation()
) extends GroupingOperation {

  /**
   * Add fill data to empty domainSet bins
   * after applying the grouping and aggregation.
   */
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
    
    val groupedData = super.applyToData(data, model)
    
    val fillData: RangeData = applyToModel(model) match {
      case Function(_, range) => range.makeFillValues
    }
    
    val range = domainSet.elements map { domain =>
      groupedData(domain) getOrElse fillData
    }
    
    SetFunction(domainSet, range)
  }
  
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
