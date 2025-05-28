package latis.ops

import cats.effect.*
import cats.syntax.all.*
import fs2.*

import latis.data.*
import latis.model.*
import latis.util.LatisException

/**
 * Combines a Stream of Samples into a Function.
 * 
 * This Aggregation Operation combines the Samples of a Dataset into a 
 * dataset with a single zero-arity sample with a MemoizedFunction
 * which contains all the samples. If the Dataset is empty, this will 
 * result in NullData.
 * 
 * In addition to being an approach to memoizing a Dataset, this Aggregation
 * can be used with a Group Operation to do the simplest aggregation: combining 
 * the samples into a nested function.
 */
class DefaultAggregation2() extends Aggregation2 {

  // The model remains unchanged since we just memoize the Dataset's Function
  def applyToModel(model: DataType): Either[LatisException, DataType] = model.asRight

  /**
   * Defines a function that puts the given Samples into a MemoizedFunction.
   */
  def aggregateFunction(model: DataType): Stream[IO, Sample] => IO[Data] = model match {
    case _: Function =>
      samples => samples.compile.toList.map(list => SampledFunction(list))
    case _ =>  //Data in range of single Sample
      samples => samples.compile.toList.map {
        case Nil  => NullData
        case list => Data.fromSeq(list.head.range)
      }
  }
  
}
