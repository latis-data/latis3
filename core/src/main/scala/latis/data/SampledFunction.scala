package latis.data

import latis.util.StreamUtils._
import fs2.Stream
import cats.effect.IO
import scala.collection.immutable.TreeMap
import latis.util.StreamUtils
import latis.resample._

/**
 * SampledFunction represent a (potentially lazy) ordered sequence of Samples.
 * Multiple implementations may be available for optimization or ease of use.
 * A SampledFunction may also be evaluated at a given domain value.
 * SampledFunctions are not generally evaluatable unless an interpolation
 * strategy is available. In such cases, Function evaluation may fail
 * with an exception (if an exact match is not found). 
 * Note that StreamFunctions are limited by being traversable once.
 */
trait SampledFunction extends Data {
  //TODO: impl scala Traversable? Monoid, Functor, Monad?
  //TODO: should default impl be moved to StreamFunction?
  //TODO: function evaluation with DomainSet, support topologies
  
  /**
   * Return a Stream of Samples from this SampledFunction.
   */
  def streamSamples: Stream[IO, Sample]
  
  /**
   * Report whether this SampledFunction has no Samples.
   */
  def isEmpty: Boolean
  
  /**
   * Evaluate this SampledFunction at the given domain value.
   * Return the result as an Option of RangeData.
   */
  def apply(
    v: DomainData, 
    interpolation: Interpolation = NoInterpolation(),
    extrapolation: Extrapolation = NoExtrapolation()
  ): Option[RangeData] = {
    //TODO: if not found, delegate to interpolation, but TraversableOnce
    val stream = streamSamples find {
      case Sample(d, _) => d == v
      case _ => false
    }
    StreamUtils.unsafeStreamToSeq(stream).headOption.map(_.range)
  }
  
  /**
   * Evaluate this SampledFunction at each point in the given DomainSet.
   * Return a SampledFunction with the new domain set and corresponding
   * range values.
   */
  def resample(
    domainSet: DomainSet, 
    interpolation: Interpolation = NoInterpolation(),
    extrapolation: Extrapolation = NoExtrapolation()
  ): SampledFunction = {
    val domainData: Seq[DomainData] = domainSet.elements
    val rangeData:  Seq[RangeData] = 
      domainData.flatMap(apply(_, interpolation, extrapolation))
    val samples = (domainData zip rangeData).map(p => Sample(p._1, p._2))
    SampledFunction.fromSeq(samples)
    //TODO: Stream?
  }
  
  /**
   * Apply the given predicate to this SampledFunction
   * to filter out unwanted Samples.
   */
  def filter(p: Sample => Boolean): SampledFunction = 
    StreamFunction(streamSamples.filter(p))
    
  /**
   * Apply the given function to this SampledFunction
   * to modify each Sample.
   */
  def map(f: Sample => Sample): SampledFunction =
    StreamFunction(streamSamples.map(f))
  
  /**
   * Apply the given function to this SampledFunction
   * to modify the Samples.
   */
  def flatMap(f: Sample => MemoizedFunction): SampledFunction =
    StreamFunction(streamSamples.flatMap(s => Stream.emits(f(s).samples)))
    
  /**
   * Reorganize this SampledFunction such that the variables
   * indicated by the given paths become the new domain. 
   * Since this may require reordering of Samples, the data
   * must be memoized. 
   * This operation is unsafe for StreamFunctions.
   */
  def groupBy(paths: SamplePath*): MemoizedFunction =
    unsafeForce.groupBy(paths: _*)
    
  /**
   * Return this SampledFunction as a MemoizedFunction.
   * It is unsafe in that a StreamFunction has to do an
   * unsafe run to get the Samples out of IO.
   */
  def unsafeForce: MemoizedFunction = this match {
    case mf: MemoizedFunction => mf
    case _ => SampledFunction.fromSeq(unsafeStreamToSeq(streamSamples))
  }

}

/**
 * Define some convenient smart constructors.
 */
object SampledFunction {
  
  def apply(samples: Sample*): MemoizedFunction = samples.length match {
    case 0 => SampledFunction.empty
    case n => SeqFunction(samples)
  }
  
  def fromSeq(samples: Seq[Sample]): MemoizedFunction = 
    SampledFunction(samples: _*)
  
  def apply(samples: Stream[IO, Sample]): SampledFunction = 
    StreamFunction(samples)
  
  val empty = EmptyFunction
  
  /**
   * Extract a Stream of Samples from a SampledFunction.
   */
  def unapply(sf: SampledFunction): Option[Stream[IO, Sample]] = {
    if (sf.isEmpty) None
    else Option(sf.streamSamples)
  }

}


/**
 * Define a SampledFunction that is always empty.
 * Note that it is a MemoizedFunction since we
 * can't safely know if a StreamFunction is empty.
 */
object EmptyFunction extends MemoizedFunction {
  def samples: Seq[Sample] = Seq.empty
}


/**
 * Experimental class to enable Dataset.cache to specify which SampledFunction 
 * implementation to use for the cache. 
 */
abstract class FunctionFactory {
  
  /**
   * Construct a SampledFunction of the implementing type
   * from a sequence of Samples.
   */
  def fromSamples(samples: Seq[Sample]): MemoizedFunction
  
  /**
   * Copy the data from the given SampledFunction to
   * a MemoizedFunction of the implementing type.
   * Default to using a potentially unsafe Seq of Samples.
   */
  def restructure(data: SampledFunction): MemoizedFunction =
    fromSamples(data.unsafeForce.samples)
    //TODO: no-op if same type
}

