package latis.ops

import latis.data.DomainData
import latis.data.Sample
import latis.model.DataType
import latis.model.Function
import latis.model.Tuple
import latis.util.LatisException

/**
 * Curry a Dataset by taking the first variable of a multi-dimensional domain
 * and making it the new domain with the rest represented as a nested Function.
 *
 * e.g. curry(a): (a, b) -> c  =>  a -> b -> c
 *
 * The effect is the restructuring of Samples such that the primary (outer)
 * Function has one Sample per curried variable value.
 *
 * Note that this will be a no-op for Datasets that already have arity one.
 * Assume no named Tuples or nested Tuples in domain, for now.
 */
case class Curry(arity: Int = 1) extends GroupOperation {
  //TODO: arity vs dimension/rank for nested tuples in the domain, ignoring nesting for now
  //TODO: Provide description for prov
  //TODO: avoid adding prov if this is a no-op

  def groupByFunction(model: DataType): Sample => Option[DomainData] = {
    if (model.arity < arity) {
      val msg = "Curry can only reduce arity"
      throw LatisException(msg)
    }
    (sample: Sample) => Some(sample.domain.take(arity))
  }

  def domainType(model: DataType): DataType = model match {
      // Ignore nested tuples
    case Function(d, _) => Tuple(d.getScalars.take(arity))
    //case Function(Tuple(es @ _*), _) => Tuple(es.take(arity))
  }

  def aggregation: Aggregation = {
    val mapOp = new MapOperation {
      override def mapFunction(model: DataType): Sample => Sample =
        (sample: Sample) => sample match {
          case Sample(d, r) => Sample(d.drop(arity), r)
        }

      override def applyToModel(model: DataType): DataType = model match {
          // Ignore nested tuples
        case Function(d, range) => Function(Tuple(d.getScalars.drop(arity)), range)
        //case Function(Tuple(es @ _*), range) => Function(Tuple(es.drop(arity)), range)
          //TODO: beef up edge cases
      }
    }

    DefaultAggregation().compose(mapOp)
  }

}
