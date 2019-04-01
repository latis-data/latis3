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
case class GroupBy(vnames: String*) extends UnaryOperation {

  /**
   * Apply the groupBy logic to the Dataset model.
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
  
  /**
   * Convert the variable information from the model into SamplePaths
   * that can be applied to the model-agnostic SampledFunction.
   */
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
    val paths = vnames.flatMap(model.getPath(_))  //TODO: deal with invalid vname
    data.groupBy(paths: _*)
  }
  
}
