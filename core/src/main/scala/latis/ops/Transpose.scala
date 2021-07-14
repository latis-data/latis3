package latis.ops

import cats.syntax.all._
import fs2.Stream

import latis.data._
import latis.model._
import latis.util.LatisException
import latis.util.LatisOrdering

/**
 * The Transpose operation swaps the two domain variables in a two-dimensional dataset.
 * {{{
 *   (x, y) -> a  =>  (y, x) -> a
 * }}}
 * Special subclasses of the affected Function (e.g. Image) or Tuple (e.g. GeoLocation)
 * will not be preserved.
 *
 * Note that this does not require the dataset to be Cartesian.
 */
class Transpose extends UnaryOperation {
  //TODO: allow arity-2 (nested tuples)
  //TODO: allow Index domain variables (not until we have Cartesian support)

  def applyToModel(model: DataType): Either[LatisException, DataType] = model match {
    case f @ Function(t @ Tuple(x: Scalar, y: Scalar), range) =>
      // Note that special subclasses of the affected Function (e.g. Image) or
      // Tuple (e.g. GeoLocation) will not be preserved.
      //TODO: do we need to munge metadata? e.g. Function shape
      for {
        tup <- Tuple.fromElements(t.id, y, x)
        f   <- Function.from(f.id, tup, range)
      } yield f
    case _ => LatisException("Transpose requires a 2D Function.").asLeft
  }

  /**
   * Transposes the two domain values then reorders the samples via a SortedSet.
   * Note that this allows transposing non-Cartesian datasets.
   */
  def applyToData(data: Data, model: DataType): Either[LatisException, Data] = data match {
    case SampledFunction(samples) => model match {
      case f @ Function(Tuple(_,_), _) =>
        val ordering = LatisOrdering.partialToTotal(LatisOrdering.sampleOrdering(f))
        //TODO: account for exception from total ordering
        val set0 = scala.collection.mutable.SortedSet[Sample]()(ordering)
        //TODO: does it help performance for this set to be mutable?
        val newSamples = samples.fold(set0) {
          case (set, Sample(DomainData(x, y), range)) => set += Sample(DomainData(y, x), range)
          case _ => ??? //error, expects 2D samples, should be caught sooner so must be inconsistent model
        }.flatMap { set =>
          Stream.emits(set.toList)
        }
        StreamFunction(newSamples).asRight
      case _ => LatisException("Transpose requires a 2D Function.").asLeft
    }
    case _ => LatisException("Transpose requires SampledFunction").asLeft
  }

}

object Transpose {

  def apply(): Transpose = new Transpose()
}
