package latis.ops

import latis.data._
import latis.model._

/**
 * Operation to restructure a Dataset by defining a new domain
 * made up of the given variables.
 * Assume that there are no nested Tuples, for now.
 * Assume that we are grouping by domain variables only, for now,
 *   so we can easily preserve complex ranges (e.g. nested functions).
 */
//case class GroupByVariables(paths: Seq[SamplePath], aggregation: Aggregation = NoAggregation()) extends GroupingOperation {
case class GroupByVariable(variableNames: String*)(val aggregation: Aggregation = NoAggregation())
    extends GroupingOperation {
  //TODO: unproject grouped variables from new range
  //TODO: error if variable doesn't exist

  def groupByFunction(model: DataType): Sample => Option[DomainData] = {
    val paths     = variableNames.flatMap(model.getPath) //TODO: deal with invalid name
    val gbIndices = paths.map(_.head).map { case DomainPosition(n) => n }
    (sample: Sample) =>
      sample match {
      case Sample(domain, _) => Option(DomainData(gbIndices.map(domain(_))))
    }
  }
//  (sample: Sample) => {
//    val datas: Seq[OrderedData] = paths.map(p => sample.getValue(p.head)) map {
//      case Some(data: OrderedData) => data
//      case Some(data) => ??? //TODO: can't use data in domain without ordering
//      case None => ??? //TODO: invalid path/sample
//    }
//    Some(DomainData.fromSeq(datas))
//  }

  /*
   * TODO: aggregation
   * do we gain anything from doing agg here as opposed to sep Op?
   * compose with Agg?
   *
   * Aggregation Op: SF => CF
   * Function (outer, named, inner?) reduced to single range value
   *   ConstantFunction: could be empty, scalar, tuple, "nested" function
   *   e.g. integration, consolidation into a SF
   * we use agg with GB to agg the Samples within each new "group"
   *   we can simply wrap them as a SeqFunction (order concerns)
   *   does that extra step cost us?
   *   don't think so, RDD agg uses Iterable of Samples
   *
   * Should GB take an agg arg?
   * *or compose: andThen? withAggregation?
   *
   * *FlatMappingOps also composes with Agg
   *   S => SF  +  SF => CF
   * what would be the common trait?
   * could live without common trait via pattern match
   * Uncurry is the only FlatMappingOp so far
   * FlatMap agg would apply to entire SF
   *
   * **not just agg, could be any Op applied to the (nested) SF
   *   e.g. unproject vars that we grouped by
   *   all of these, including agg, are applied to newly created SF, for groupBy
   *   not simply composition
   *   BG.with(op)?
   *   BG alone will make a nested SF
   *     with(op) would apply that op to those nested functions (one per outer sample)
   *     what would the result Op type be?
   *       still GB? but couldn't apply "with" again?
   *         it could be applied to a CF
   *       would still be a GroupingOp since it makes a new domain set
   *       maybe need optional arg for Op
   * agging to a SF should be part of GB
   *   RDD has special needs
   *   an agg/op here could be applied to RDD via mapValues?
   *
   * could this agg/op be used to declare the overall function cartesian?
   *
   */

  /**
   * Apply the groupBy logic to the Dataset model.
   */
  override def applyToModel(model: DataType): DataType = model match {
    case Function(domain, range) =>
      val (outer, inner) = domain.getScalars.partition(s => variableNames.contains(s.id))
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
