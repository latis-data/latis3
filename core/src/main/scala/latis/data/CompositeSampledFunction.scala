package latis.data

import fs2.Stream
import cats.effect.IO

/**
 * Define a SampledFunction that consists of a sequence of SampledFunctions.
 * This is useful when appending data granules.
 * It is assumed that the model is the same for each granule
 * and that the Samples are ordered such that they can be concatenated 
 * and preserve the ordering.
 */
case class CompositeSampledFunction(
  sampledFunctions: Seq[SampledFunction]
) extends SampledFunction {
  //TODO: flatten so we don't end up with nested CSFs?
  
  /**
   * Stream Samples by simply concatenating Samples from the component
   * SampledFunctions.
   */
  def streamSamples: Stream[IO, Sample] =
    sampledFunctions.map(_.streamSamples).fold(Stream.empty)(_ ++ _)
    
  /**
   * A CompositeSampledFunction is empty if it has no component
   * SampledFunctions or each component SampledFunction is empty.
   */
  def isEmpty: Boolean = sampledFunctions.exists(! _.isEmpty)
  
  /**
   * Override filter by delegating the predicate application
   * to each component SampledFunction.
   */
  override def filter(p: Sample => Boolean): SampledFunction = 
    CompositeSampledFunction(sampledFunctions.map(_.filter(p)))
    
  /**
   * Override map by delegating the function application
   * to each component SampledFunction.
   */
  override def map(f: Sample => Sample): SampledFunction =
    CompositeSampledFunction(sampledFunctions.map(_.map(f)))
    
  /**
   * Override flatMap by delegating the function application
   * to each component SampledFunction.
   */
  override def flatMap(f: Sample => MemoizedFunction): SampledFunction =
    CompositeSampledFunction(sampledFunctions.map(_.flatMap(f)))
    
  
  //TODO: optimize other operations by delegating to granules; e.g. select, project
}

object CompositeSampledFunction {
  
  def apply(sf1: SampledFunction, sfs: SampledFunction*) =
    new CompositeSampledFunction(sf1 +: sfs)
}