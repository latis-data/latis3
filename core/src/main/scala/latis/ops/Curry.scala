package latis.ops

import cats.syntax.all._

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
 * e.g. curry(1): (a, b) -> c  =>  a -> b -> c
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
      val msg = "Curry can only reduce arity. Use Uncurry to increase arity."
      throw LatisException(msg)
    }
    (sample: Sample) => Some(sample.domain.take(arity))
  }

  // takes the model for the dataset and returns the domain of the curried dataset
  def domainType(model: DataType): DataType = model match {
    // Ignore nested tuples
    case Function(d, _) =>
      d.getScalars.take(arity) match {
        case s1 :: Nil => s1
        case ss        => Tuple(ss)
      }
    //case Function(Tuple(es @ _*), _) => Tuple(es.take(arity))
  }

  def aggregation: Aggregation = {
    val mapOp = new MapOperation {
      override def mapFunction(model: DataType): Sample => Sample = {
        case Sample(d, r) => Sample(d.drop(arity), r)
      }

      // takes the model for the dataset and returns the range of the curried dataset
      override def applyToModel(model: DataType): Either[LatisException, DataType] = model match {
        // Ignore nested tuples
        case Function(d, range) =>
          (d.getScalars.drop(arity) match {
            case Nil       => range // this happens when the arity is not changed
            case s1 :: Nil => Function(s1, range)
            case ss        => Function(Tuple(ss), range)
          }).asRight
        case _ => Left(LatisException("Model must be a function"))
        //case Function(Tuple(es @ _*), range) => Function(Tuple(es.drop(arity)), range)
        //TODO: beef up edge cases
      }
    }

    DefaultAggregation().compose(mapOp)
  }
}

object Curry {
  def fromArgs(args: List[String]): Either[LatisException, Curry] = args match {
    case arity :: Nil =>
      Either
        .catchOnly[NumberFormatException](Curry(arity.toInt))
        .leftMap(LatisException(_))
    case Nil => Right(Curry())
    case _   => Left(LatisException("Too many arguments to Curry"))
  }
}
