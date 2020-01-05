package latis.ops

import latis.data._
import latis.model._
import latis.util.LatisException

/**
 * Operation to restructure a Dataset by defining a new domain
 * made up of the given variables.
 * Assume that there are no nested Tuples, for now.
 * Assume that we are grouping by domain variables only, for now,
 *   so we can easily preserve complex ranges (e.g. nested functions).
 */
case class GroupByVariable(variableNames: String*) extends GroupOperation {
  //TODO: unproject grouped variables from new range

  def aggregation: Aggregation = {
    val mapOp: MapOperation = ??? //TODO: unproject
    DefaultAggregation().compose(mapOp)
  }

  /**
   * Gets the SamplePath for each group-by variable.
   */
  def samplePositions(model: DataType): List[SamplePosition] = variableNames.toList.map { vname =>
    model.getPath(vname) match {
      case Some(path) =>
        if (path.length > 1)
          throw LatisException(s"Group-by variable must not be in a nested Function: $vname")
        else path.head
      case None =>
        throw LatisException(s"Group-by variable not found: $vname")
    }
  }

  def domainType(model: DataType): DataType = {
    val scalars = variableNames.map { vname =>
      model.findVariable(vname) match {
        case Some(scalar: Scalar) => scalar
        case Some(_) => throw LatisException(s"Group-by variable must be a Scalar: $vname")
        //TODO: support grouping by a tuple, e.g. location?
        case None => ??? //bug since we checked above
      }
    }
    Tuple(scalars).flatten
  }

  def groupByFunction(model: DataType): Sample => Option[DomainData] =
    (sample: Sample) => {
      val data: List[Datum] = samplePositions(model).map(sample.getValue).map {
        case Some(d: Datum) => d
        case _ => ??? //invalid sample
      }
      Option(DomainData(data))
    }

}
