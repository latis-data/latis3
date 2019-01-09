package latis.data

import latis.util.StreamUtils._
import scala.collection.immutable.TreeMap
import cats.effect.IO
import fs2.Stream

import scala.language.postfixOps
    
/**
 * This trait is implemented by SampledFunctions that
 * are not tied to an external data source, e.g. they hold
 * all data in memory.
 */
trait MemoizedFunction extends SampledFunction {
  
  /**
   * A MemoizedFunction should be able to provide a sequence of Samples.
   */
  def samples: Seq[Sample]
  
  /**
   * A MemoizedFunction is empty if it has no Samples.
   */
  def isEmpty: Boolean = samples.isEmpty
  
  /**
   * Implement SampledFunction contract to provide Stream of Samples.
   * Do this in terms of the "samples" Seq so all MemoizedFunction sub-types
   * inherit the capability.
   */
  def streamSamples: Stream[IO, Sample] = seqToIOStream(samples)
  
  /**
   * Evaluate this SampledFunction at the given domain value.
   * Return the result as a SampledFunction with one Sample.
   */
  /*
   * TODO: inconvenient for use
   * to get single range value: head.range.head
   * consider function composition
   */
  override def apply(value: DomainData): MemoizedFunction = {
    //TODO: implicit Interpolation strategy
    //TODO: take advantage of ordering, find should at least short-circuit
    val z = samples find {
      case Sample(d, _) => DomainOrdering.equiv(d, value) //Note, can't use "=="
      case _ => false
    }
 //TODO: try   z.fold(SampledFunction.empty)(s => SampledFunction(s))
    //see https://github.com/latis-data/latis3/pull/3#discussion_r243677545
    
    z match {
      case Some(sample) => SampledFunction(sample)
      case None => SampledFunction.empty
    }
  }
  
  
  override def filter(p: Sample => Boolean): MemoizedFunction = 
    SampledFunction.fromSeq(samples.filter(p))
    
  override def map(f: Sample => Sample): MemoizedFunction = 
    SampledFunction.fromSeq(samples.map(f))
    
  override def flatMap(f: Sample => MemoizedFunction): MemoizedFunction = 
    SampledFunction.fromSeq(samples.flatMap(s => f(s).samples))
    
  def head: Sample = samples.head
  
  /*
   * explore groupBy
   * will always produce nested function?
   *   may be special case of var having one-to-one mapping (injection) with current domain
   *   can we determine that here and optimize?
   *   or do we need a diff operation?
   * could do with StreamFunction
   *   unsafe since we need to suck all into memory to guarantee sorting
   *   if there is no reordering then use Curry
   * uncurry if a gb value is in a nested function
   * if var is in a domain, even nested, no need to introduce index?
   *   if not cartesian, could have sparse data with fill values
   * if var is in range need to introduce index since we can't guarantee uniqueness
   */
  /**
   * Group the SampledFunction by the new domain values expressed by "paths".
   * Assume there are no nested Functions (uncurried) and that the new domain
   * is a subset of the original domain. 
   */
  override def groupBy(paths: SamplePath*): MemoizedFunction = {
    //assume uncurried with grouping vars in domain only, for now
    //TODO: deal with invalid positions
    implicit val ord = DomainOrdering
    var map = TreeMap[DomainData, Seq[Sample]]()
    
    // Get the indices into the original domain for the new outer domain
    val outerIndices: Array[Int] = paths.map(_.head) map { case DomainPosition(n) => n } toArray
    
    samples foreach {
      case Sample(domain, range) =>
        val outerDomain: DomainData = DomainData.fromSeq(outerIndices.map(domain(_)))
        val innerDomain: DomainData = //TODO: optimize, do it once, need length of domain
          domain.zipWithIndex.filter(p => ! outerIndices.contains(p._2)).map(_._1)
          
        //put into SortedMap and accumulate sorted seq of range values
        val rangeSamples = map.get(outerDomain) match {
          case Some(ss: Seq[Sample]) => ss :+ Sample(innerDomain, range)
          case None => Seq(Sample(innerDomain, range))
        }
        map = map + (outerDomain -> rangeSamples)
    }
    
    // Build SampledFunction from range samples
    //TODO: use MapFunction?
    SampledFunction.fromSeq( map map { p => Sample(p._1, Array(SampledFunction.fromSeq(p._2))) } toSeq )
  }
}

object MemoizedFunction {
  
  /**
   * Extract a Seq of Samples from a SampledFunction.
   * If the SampledFunction
   */
  def unapply(sf: SampledFunction): Option[Seq[Sample]] = sf match {
    case mf: MemoizedFunction => 
      if (mf.isEmpty) None 
      else Option(mf.samples)
    case _ => None //can't expose Seq from a non-memoized Function
  }

}