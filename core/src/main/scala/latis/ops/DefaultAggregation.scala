package latis.ops

import latis.data._
import latis.model._

/**
 * Defines an Operation that combines all the Samples
 * of a Dataset into a MemoizedFunction in the range of
 * a ConstantFunction.
 */
case class DefaultAggregation() extends Aggregation {

  /**
   * Does not change the model for DefaultAggragtion.
   * The input Data is lifted into the range of a
   * ConstantFunction so the type does not change.
   */
  def applyToModel(model: DataType): DataType = model

  /**
   * Defines a function that puts the given Samples into
   * a MemoizedFunction.
   */
  def aggregateFunction(model: DataType): Iterable[Sample] => Data = model match {
    case _: Function =>
      (samples: Iterable[Sample]) => SeqFunction(samples.toIndexedSeq)
    case _ =>
      // Not a function implies a single range value
      (samples: Iterable[Sample]) => Data.fromSeq(samples.head.range)
  }
}
