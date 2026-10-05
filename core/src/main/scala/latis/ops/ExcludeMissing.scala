package latis.ops

import cats.syntax.all.*

import latis.data.*
import latis.model.*
import latis.util.LatisException

/**
 * Operation to keep only Samples that are not missing data
 * 
 * This operation acts upon an entire Dataset, and recurses into each Sample to check if the data
 * it contains is considered missing. Note that this operation does not work with nested tuples 
 * but does work with nested functions. If any data is considered missing, the Sample will be 
 * removed. Data is considered missing according to the return value of `isMissing`.
 * 
 * As a Filter, a predicate determines the fate of a Sample based only on the state of that Sample.
 */
case class ExcludeMissing() extends Filter {
  def predicate(model: DataType): Either[LatisException, Sample => Boolean] = {
    def go(dt: DataType, range: RangeData): Either[LatisException, Boolean] = {
      dt match {
        case s: Scalar =>
          if (range.length == 1) {
            s.isMissing(range(0)).asRight
          } else {
            LatisException("Expected range of Sample for a Scalar to hold one value").asLeft
          }
        case t: Tuple =>
          t.elements.zip(range).map { (dt, ds) =>
            go(dt, List(ds))
          }.sequence.map { boolList => boolList.exists(identity)}
        case Function(_, rn) =>
          range match {
            case List(SeqFunction(List(func), _)) =>
              func match {
                case (_, r: List[_]) =>
                  go(rn, r.asInstanceOf[List[Data]])
                case _ =>
                  LatisException("Expected SeqFunction to contain a tuple of List[Data]").asLeft
              }
            case _ => go(rn, range)
          }
      }
    }
      
    val boolResult = (sample: Sample) => 
      go(model, sample.range) match {
        case Left(err) => throw err
        case Right(bool) => bool
      }

    boolResult.asRight[LatisException]
  }
}

object ExcludeMissing {
  def builder: OperationBuilder = (args: List[String]) =>
    Either.cond(
      args.isEmpty,
      ExcludeMissing(),
      LatisException("ExcludeMissing takes no arguments")
    )
}
