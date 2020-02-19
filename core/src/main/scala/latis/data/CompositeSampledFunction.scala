package latis.data

import cats.effect.IO
import fs2.Stream

import latis.model.DataType
import latis.ops.UnaryOperation
import latis.util.LatisException

/**
 * Define a SampledFunction that consists of a sequence of SampledFunctions.
 * This is useful when appending data granules.
 * It is assumed that the model is the same for each granule
 * and that the Samples are ordered such that they can be concatenated
 * and preserve the ordering.
 */
case class CompositeSampledFunction(sampledFunctions: Seq[SampledFunction])
  extends SampledFunction {
  //TODO: flatten so we don't end up with nested CSFs?
  //TODO: make sure extrapolation is not enabled on SFs

  /**
   * Stream Samples by simply concatenating Samples from the component
   * SampledFunctions.
   */
  def samples: Stream[IO, Sample] =
    sampledFunctions.map(_.samples).fold(Stream.empty)(_ ++ _)

  def apply(data: DomainData): Either[LatisException, RangeData] = {
    // Use result from first SF that provides one
    sampledFunctions.map { sf =>
      sf(data)
    }.collectFirst {
      case r if r.isRight => r
    }.getOrElse {
      val msg = s"No sample found matching $data"
      Left(LatisException(msg))
    }
  }

  /**
   * A CompositeSampledFunction is empty if it has no component
   * SampledFunctions or each component SampledFunction is empty.
   */
//  def isEmpty: Boolean = sampledFunctions.forall(_.isEmpty)
  //Note: forall does return true if the Seq is empty.

  /*
   * TODO: Some operations won't behave the same if we delegate to the granules.
   * e.g. stride (using filter), unless each granule sample count is a multiple of the stride
   * presumably we can't simply override these base methods
   * can we capture this property as a trait?
   * distributive? associative?, monoidal, but binary
   * but unary is just partially applied binary
   */

  override def applyOperation(op: UnaryOperation, model: DataType): SampledFunction =
    CompositeSampledFunction(sampledFunctions.map(_.applyOperation(op, model)))

  ///**
  // * Override filter by delegating the predicate application
  // * to each component SampledFunction.
  // */
  //override def filter(p: Sample => Boolean): SampledFunction =
  //  CompositeSampledFunction(sampledFunctions.map(_.filter(p)))
  //
  ///**
  // * Override map by delegating the function application
  // * to each component SampledFunction.
  // */
  //override def map(f: Sample => Sample): SampledFunction =
  //  CompositeSampledFunction(sampledFunctions.map(_.map(f)))
  //
  ///**
  // * Override flatMap by delegating the function application
  // * to each component SampledFunction.
  // */
  //override def flatMap(f: Sample => MemoizedFunction): SampledFunction =
  //  CompositeSampledFunction(sampledFunctions.map(_.flatMap(f)))
  ////TODO: optimize other operations by delegating to granules; e.g. select, project
}

object CompositeSampledFunction {

  def apply(sf1: SampledFunction, sfs: SampledFunction*) =
    new CompositeSampledFunction(sf1 +: sfs)
}
