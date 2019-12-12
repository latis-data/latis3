package latis.data

import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.language.postfixOps

import cats.effect.IO
import fs2.Stream

import latis.util.StreamUtils._

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
   * Return the result as an Option of RangeData.
   */
  override def apply(value: DomainData): Option[RangeData] = {
    val osample: Option[Sample] = samples.find {
      case Sample(d, _) => d.equals(value)
      case _ => false
    }
    osample.map(_.range)
  }

  override def filter(p: Sample => Boolean): MemoizedFunction =
    SampledFunction(samples.filter(p))

  override def map(f: Sample => Sample): MemoizedFunction =
    SampledFunction(samples.map(f))

  override def flatMap(f: Sample => MemoizedFunction): MemoizedFunction =
    SampledFunction(samples.flatMap(s => f(s).samples))

  def head: Sample = samples.head

  /**
   * Group the SampledFunction by the new domain values expressed by "paths".
   * Assume there are no nested Functions (uncurried) and that the new domain
   * is a subset of the original domain.
   */
  override def groupBy(paths: SamplePath*): MemoizedFunction = {
    //TODO: deal with invalid positions

    val sampleMap = mutable.LinkedHashMap[DomainData, Seq[Sample]]()

    // Get the indices into the original domain for the new outer domain
    val outerIndices: Array[Int] = paths.map(_.head).map { case DomainPosition(n) => n } toArray

    samples.foreach {
      case Sample(domain, range) =>
        val outerDomain: DomainData = DomainData(outerIndices.map(domain(_)))
        val innerDomain: DomainData = //TODO: optimize, do it once, need length of domain
          domain.zipWithIndex.filterNot(p => outerIndices.contains(p._2)).map(_._1)

        //put into SortedMap and accumulate sorted seq of range values
        val rangeSamples = sampleMap.get(outerDomain) match {
          case Some(ss: Seq[Sample]) => ss :+ Sample(innerDomain, range)
          case None => Seq(Sample(innerDomain, range))
        }
        sampleMap + (outerDomain -> rangeSamples)
    }

    // Build SampledFunction from range samples
    //TODO: use MapFunction?
    SampledFunction(sampleMap.map { p =>
      Sample(p._1, Array(SampledFunction(p._2)))
    } toSeq)
  }

  /**
   * Join two SampledFunctions assuming they have the same domain set.
   */
  def join(that: MemoizedFunction): MemoizedFunction = {
    val samples = this.samples.zip(that.samples).map {
      case (Sample(d, r1), Sample(_, r2)) => Sample(d, r1 ++ r2)
    }
    SeqFunction(samples)
  }

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
   * If the SampledFunction
   */
  def unapply(sf: SampledFunction): Option[Seq[Sample]] = sf match {
    case mf: MemoizedFunction =>
      if (mf.isEmpty) None
      else Option(mf.samples)
    case _ => None //can't expose Seq from a non-memoized Function
  }

}
