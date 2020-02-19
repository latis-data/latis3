package latis.ops

import cats.effect.IO
import fs2.Pipe
import fs2.Stream

import latis.data.ConstantFunction
import latis.data.DomainData
import latis.data.MemoizedFunction
import latis.data.RangeData
import latis.data.Sample
import latis.data.SeqFunction
import latis.model.DataType

/**
 * Defines an Operation that applies a fold to a Dataset.
 * The result of the fold will be a Dataset with a 0-arity Function.
 * This may represent a constant or a nested MemoizedFunction.
 */
trait FoldOperation extends StreamOperation {

  //TODO: applyToModel, up to subclass?

  /**
   * Defines the default zero for a MemoizedFunction
   * as an empty SeqFunction.
   */
  def zero: MemoizedFunction = SeqFunction(Seq.empty)

  /**
   * Defines the function to be used in the fold operation.
   */
  def foldFunction: (MemoizedFunction, Sample) => MemoizedFunction

  /**
   * Folds the given Stream of Samples into a Stream with a single
   * zero-arity Sample.
   */
  override def pipe(model: DataType): Pipe[IO, Sample, Sample] =
    (samples: Stream[IO, Sample]) =>
      samples.fold(zero)(foldFunction).map {
        // Avoid a nested ConstantFunction //TODO: not allowed?
        case ConstantFunction(range) => Sample(DomainData(), RangeData(range))
        // Otherwise keep as a nested function
        case mf => Sample(DomainData(), RangeData(mf))
      }

}
