package latis.ops

import cats.syntax.all._

import latis.data._
import latis.model._
import latis.util.LatisException

/**
 * Defines an Operation that combines all the Samples
 * of a Dataset into a MemoizedFunction in the range
 * of a single zero-arity Sample.
 */
case class DefaultAggregation() extends Aggregation {
  /**
   * Does not change the model for DefaultAggregation.
   */
  def applyToModel(model: DataType): Either[LatisException, DataType] = model.asRight

  /**
   * Defines a function that puts the given Samples into
   * a MemoizedFunction.
   */
  def aggregateFunction(model: DataType): Iterable[Sample] => Data = model match {
    case _: Function =>
      (samples: Iterable[Sample]) => SeqFunction(samples.toIndexedSeq)
    case _ =>
      // Not a function implies a single range value
      //TODO: error is samples is empty? NullData?
      (samples: Iterable[Sample]) => Data.fromSeq(samples.head.range)
  }
}
