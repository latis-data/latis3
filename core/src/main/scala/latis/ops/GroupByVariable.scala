package latis.ops

import cats.implicits._

import latis.data._
import latis.model._
import latis.util.Identifier
import latis.util.LatisException

/**
 * Defines an Operation to restructure a Dataset by defining a new domain
 * made up of the given variables. The samples in each group will have
 * the grouping variables dropped then wrapped as a nested MemoizedFunction.
 * Assumes there are no nested function, for now.
 * Does not preserve nested Tuples, for now.
 */
case class GroupByVariable(ids: Identifier*) extends GroupOperation {

  /**
   * Defines a DefaultAggregation composed with a MapOperation that un-projects
   * the group-by variables. The Samples in each group will have the group-by
   * variables removed then wrapped as a SampledFunction.
   */
  def aggregation: Aggregation =
    DefaultAggregation().compose(RemoveGroupedVariables(ids))

  /**
   * Gets the SamplePosition for each group-by variable.
   */
  def samplePositions(model: DataType): List[SamplePosition] = ids.toList.map { id =>
    model.getPath(id) match {
      case Some(path) =>
        if (path.length > 1)
          throw LatisException(s"Group-by variable must not be in a nested Function: ${id.asString}")
        else path.head
      case None =>
        throw LatisException(s"Group-by variable not found: ${id.asString}")
    }
  }

  def domainType(model: DataType): DataType = {
    val scalars = ids.map { vname =>
      model.findVariable(vname) match {
        case Some(scalar: Scalar) => scalar
        case Some(_) => throw LatisException(s"Group-by variable must be a Scalar: ${vname.asString}")
        //TODO: support grouping by a tuple, e.g. location?
        case None =>
          //TODO: validate variables eagerly
          val msg = s"Invalid variable name: ${vname.asString}"
          throw LatisException(msg)
      }
    }
    Tuple.fromSeq(scalars).fold(throw _, identity)  //TODO: .flatten?
  }

  def groupByFunction(model: DataType): Sample => Option[DomainData] =
    (sample: Sample) => {
      val data: List[Datum] = samplePositions(model).map(sample.getValue).map {
        case Some(d: Datum) => d
        case _ => throw LatisException("Invalid Sample")
      }
      Option(DomainData(data))
    }

}

/**
 * Defines an operation akin to un-projection which can safely drop the
 * grouped variables from domains since all values for that dimension
 * should be the same in each group.
 */
case class RemoveGroupedVariables(ids: Seq[Identifier]) extends MapOperation {

  override def applyToModel(model: DataType): Either[LatisException, DataType] =
    applyToVariable(model).toRight(LatisException("ids filtered entire model."))

  /** Recursive method to build new model by dropping variableNames. */
  private def applyToVariable(v: DataType): Option[DataType] = v match {
    case s: Scalar =>
      if (ids.contains(s.id)) None else Some(s)
    case t @ Tuple(vars @ _*) =>
      val vs = vars.flatMap(applyToVariable)
      vs.length match {
        case 0 => None // drop empty Tuple
        case 1 => Some(vs.head) // reduce Tuple of one
        case _ => Some(Tuple.fromSeq(t.id, vs).fold(throw _, identity))
      }
    case f @ Function(d, r) =>
      (applyToVariable(d), applyToVariable(r)) match {
        case (Some(d), Some(r)) => Some(Function.from(f.id, d, r).fold(throw _, identity))
        case (None, Some(r)) => Some(r)
        //TODO: deal with empty range
        case _ => None
      }
  }

  override def mapFunction(model: DataType): Sample => Sample = {
    // Determine the list of variables to keep
    val vnames: List[Identifier] = model.getScalars.map(_.id).filterNot(ids.contains)

    // Get the paths of the variables to be removed from each Sample.
    // Sort to maintain the original order of variables.
    val samplePositions = vnames.flatMap(model.getPath).map(_.head)
    val domainIndices: Seq[Int] = samplePositions.collect {
      case DomainPosition(i) => i
    }.sorted
    val rangeIndices: Seq[Int] = samplePositions.collect {
      case RangePosition(i)  => i
    }.sorted

    (sample: Sample) => sample match {
      case Sample(ds, rs) =>
        val domain = domainIndices.map(ds(_))
        val range = rangeIndices.map(rs(_))
        Sample(domain, range)
    }
  }
}
