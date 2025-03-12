package latis.ops

import cats.effect.*
import cats.syntax.all.*
import fs2.*

import latis.data.*
import latis.model.*
import latis.util.LatisException

/**
 * Defines an Operation that combines all the Samples
 * of a Dataset into a MemoizedFunction in the range
 * of a single zero-arity Sample.
 * 
 * Aggregations are most useful when combined with a group operation.
 */
case class DefaultAggregation2() extends Aggregation2 {

  // Model remains unchanged
  def applyToModel(model: DataType): Either[LatisException, DataType] = model.asRight

  /**
   * Defines a function that puts the given Samples into a MemoizedFunction.
   */
  def aggregateFunction(model: DataType): Stream[IO, Sample] => IO[Data] = model match {
    case _: Function =>
      samples => samples.compile.toList.map(list => SampledFunction(list))
    case _ =>  //Data in range of single Sample
      //TODO: error if samples is empty
      samples => samples.compile.toList.map(list => Data.fromSeq(list.head.range))
  }
}
