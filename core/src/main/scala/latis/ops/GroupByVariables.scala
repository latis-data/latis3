package latis.ops

import latis.model._
import latis.metadata._
import latis.data._

/**
 * Operation to restructure a Dataset by defining a new domain
 * made up of the given variables.
 * Assume that there are no nested Tuples, for now.
 * Assume that we are grouping by domain variables only, for now,
 *   so we can easily preserve complex ranges (e.g. nested functions).
 */
//case class GroupByVariables(paths: Seq[SamplePath], aggregation: Aggregation = NoAggregation()) extends GroupingOperation {
case class GroupByVariables(vnames: String*)(val aggregation: Aggregation = NoAggregation()) extends GroupingOperation {
  //TODO: unproject grouped variables from new range
  
  def groupByFunction: Sample => Option[DomainData] = ???
//  (sample: Sample) => {
//    val datas: Seq[OrderedData] = paths.map(p => sample.getValue(p.head)) map {
//      case Some(data: OrderedData) => data
//      case Some(data) => ??? //TODO: can't use data in domain without ordering
//      case None => ??? //TODO: invalid path/sample
//    }
//    Some(DomainData.fromSeq(datas))
//  }

  /**
   * Apply the groupBy logic to the Dataset model.
   */
  /*
   * TODO: model doesn't understand sample paths
   * go back to constructing with vnames
   * applyToData will need to be overridden but could still delegate to super for most?
   * 
   */
  override def applyToModel(model: DataType): DataType = model match {
    case Function(domain, range) =>
      val (outer, inner) = domain.getScalars.partition(s => vnames.contains(s.id))
      val innerDomain = inner.length match {
        case 0 => ??? //TODO: all domain vars regrouped, implies reordering with no new Function
        case 1 => inner.head
        case _ => Tuple(inner: _*)
      }
      val outerDomain = outer.length match {
        case 0 => ??? //TODO: error, no gb vars found
        case 1 => outer.head
        case _ => Tuple(outer: _*)
      }
      Function(outerDomain, Function(innerDomain, range))
    case _ => ??? //TODO: error, model must be a Function
  }
  
//  /**
//   * Convert the variable information from the model into SamplePaths
//   * that can be applied to the model-agnostic SampledFunction.
//   */
//  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
//    val paths = vnames.flatMap(model.getPath(_))  //TODO: deal with invalid vname
//    data.groupBy(paths: _*)
//  }
  
}
