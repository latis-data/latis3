package latis.data

import latis.util.StreamUtils._
import fs2.Stream
import cats.effect.IO
import scala.collection.immutable.TreeMap

/**
 * SampledFunction represent a (potentially lazy) ordered sequence of Samples.
 * Multiple implementations may be available for optimization or ease of use.
 * A SampledFunction may also be evaluated at a given domain value.
 * SampledFunctions are not generally evaluatable unless an interpolation
 * strategy is available. In such cases, Function evaluation may fail
 * with an exception (if an exact match is not found). 
 * Note that StreamFunctions are limited by being traversable once.
 */
trait SampledFunction {
  //TODO: impl scala Traversable? Monoid, Functor, Monad?
  //TODO: should default impl be moved to StreamFunction?
  
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
   * Return the result as a SampledFunction with one Sample.
   */
  def apply(v: DomainData): SampledFunction = {
    /*
     * TODO: reconsider return type
     * consider function composition
     * we generally only want the corresponding range value
     * compare to how apply(vs: DomainSet) might work
     */
    //TODO: implicit Interpolation strategy
    val stream = streamSamples find {
      case Sample(d, _) => d == v
      case _ => false
    }
    StreamFunction(stream)
  }
  //TODO: eval with DomainSet, support topologies => SampledFunction
  
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
  //TODO "Builder" like other scala Builders?
  def fromSeq(samples: Seq[Sample]): MemoizedFunction
}

