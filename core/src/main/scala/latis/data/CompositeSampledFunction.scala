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
  
  def streamSamples: Stream[IO, Sample] =
    sampledFunctions.map(_.streamSamples).fold(Stream.empty)(_ ++ _)
    
  def isEmpty: Boolean = sampledFunctions.exists(! _.isEmpty)
    
  //TODO: optimize other operations by delegating to granules
}

object CompositeSampledFunction {
  
  def apply(sf1: SampledFunction, sfs: SampledFunction*) =
    new CompositeSampledFunction(sf1 +: sfs)
}