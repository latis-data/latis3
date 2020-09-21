package latis.data

import cats.effect.IO
import fs2.Stream

import latis.util.DefaultDomainOrdering
import latis.util.LatisException
import latis.util.StreamUtils._

/**
 * Defines a trait for SampledFunctions that
 * are not tied to an external data source,
 * e.g. they hold all data in memory or some
 * other form of cache.
 */
trait MemoizedFunction extends SampledFunction {

  /**
   * Provides a sequence of Samples.
   */
  def sampleSeq: Seq[Sample]

  /**
   * Returns the number of Samples in this MemoizedFunction.
   */
  def length: Int = sampleSeq.length

  /**
   * A MemoizedFunction is empty if it has no Samples.
   */
  def isEmpty: Boolean = sampleSeq.isEmpty

  /**
   * Implement SampledFunction contract to provide Stream of Samples.
   * Do this in terms of the "samples" Seq so all MemoizedFunction sub-types
   * inherit the capability.
   */
  def samples: Stream[IO, Sample] = seqToIOStream(sampleSeq)

  /**
   * Evaluates this SampledFunction at the given domain value.
   * This will use this SampledFunction's Ordering (or the default
   * if it doesn't have one) only for equivalence. This implementation
   * can't ensure a Cartesian topology to safely use a binary search.
   */
  override def eval(data: DomainData): Either[LatisException, RangeData] = {
    val eq: Equiv[DomainData] = ordering.getOrElse(DefaultDomainOrdering)
    val osample: Option[Sample] = sampleSeq.find {
      case Sample(d, _) => eq.equiv(d, data)
      case _ => false
    }
    osample match {
      case Some(s) => Right(s.range)
      case None =>
        val msg = s"No sample found matching: $data"
        Left(LatisException(msg))
    }
  }

  //
  //override def filter(p: Sample => Boolean): MemoizedFunction =
  //  SampledFunction(samples.filter(p))
  //
  //override def map(f: Sample => Sample): MemoizedFunction =
  //  SampledFunction(samples.map(f))
  //
  //override def flatMap(f: Sample => MemoizedFunction): MemoizedFunction =
  //  SampledFunction(samples.flatMap(s => f(s).samples))
  //
  //def head: Sample = samples.head
  //
  ///**
  // * Group the SampledFunction by the new domain values expressed by "paths".
  // * Assume there are no nested Functions (uncurried) and that the new domain
  // * is a subset of the original domain.
  // */
  //override def groupBy(paths: SamplePath*)(implicit ordering: Ordering[DomainData]): MemoizedFunction = {
  //  //TODO: deal with invalid positions
  //
  //  val sampleMap = mutable.LinkedHashMap[DomainData, Seq[Sample]]()
  //
  //  // Get the indices into the original domain for the new outer domain
  //  val outerIndices: Array[Int] = paths.map(_.head).map { case DomainPosition(n) => n } toArray
  //
  //  samples.foreach {
  //    case Sample(domain, range) =>
  //      val outerDomain: DomainData = DomainData(outerIndices.map(domain(_)))
  //      val innerDomain: DomainData = //TODO: optimize, do it once, need length of domain
  //        domain.zipWithIndex.filterNot(p => outerIndices.contains(p._2)).map(_._1)
  //
  //      //put into SortedMap and accumulate sorted seq of range values
  //      val rangeSamples = sampleMap.get(outerDomain) match {
  //        case Some(ss: Seq[Sample]) => ss :+ Sample(innerDomain, range)
  //        case None => Seq(Sample(innerDomain, range))
  //      }
  //      sampleMap + (outerDomain -> rangeSamples)
  //  }
  //
  //  // Build SampledFunction from range samples
  //  //TODO: use MapFunction?
  //  SampledFunction(sampleMap.map { p =>
  //    Sample(p._1, Array(SampledFunction(p._2)))
  //  } toSeq)
  //}
  //
  ///**
  // * Join two SampledFunctions assuming they have the same domain set.
  // */
  //def join(that: MemoizedFunction): MemoizedFunction = {
  //  val samples = this.samples.zip(that.samples).map {
  //    case (Sample(d, r1), Sample(_, r2)) => Sample(d, r1 ++ r2)
  //  }
  //  SeqFunction(samples)
  //}
  //
  // Use a SortedMapFunction
  //override def union(that: SampledFunction): SampledFunction = that match {
  //  case mf: MemoizedFunction =>
  //    val builder: mutable.Builder[(DomainData, RangeData), SortedMap[DomainData, RangeData]] =
  //      SortedMap.newBuilder
  //    this.samples.foreach(s => builder += s)
  //    mf.samples.foreach(s   => builder += s) //TODO: test keep the original if duplicates
  //    SortedMapFunction(builder.result())
  //  case _ => ??? //TODO: union as Streams
  //}
}

object MemoizedFunction {

  /**
   * Extract a Seq of Samples from a SampledFunction.
   */
  def unapply(sf: SampledFunction): Option[Seq[Sample]] = sf match {
    case mf: MemoizedFunction =>
      if (mf.isEmpty) None
      else Option(mf.sampleSeq)
    case _ => None //can't expose Seq from a non-memoized Function
  }

}
