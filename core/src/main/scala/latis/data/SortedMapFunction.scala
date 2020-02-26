package latis.data

import scala.collection.immutable.SortedMap

import latis.util.DefaultDomainOrdering
import latis.util.LatisException
import latis.util.LatisOrdering

/**
 * Implements a SampledFunction in terms of a SortedMap.
 * This takes advantage of the Map for evaluation.
 */
case class SortedMapFunction(
  sortedMap: SortedMap[DomainData, RangeData]
) extends MemoizedFunction {
  //TODO: use nested SortedMaps for nD datasets

  /**
   * Provides the ordering for this SampledFunction from the
   * SortedMap that implements it.
   */
  def ordering: Option[PartialOrdering[DomainData]] = Some(sortedMap.ordering)

  def sampleSeq: Seq[Sample] = sortedMap.toSeq

  override def apply(value: DomainData): Either[LatisException, RangeData] =
    sortedMap.get(value) match {
      case Some(r) => Right(r)
      case None =>
        val msg = s"No sample found matching $value"
        Left(LatisException(msg))
    }
  //TODO: support interpolation

  //TODO: optimize other methods
}

object SortedMapFunction { //extends FunctionFactory {

  def fromSamples(
    samples: Seq[Sample],
    ordering: PartialOrdering[DomainData] = DefaultDomainOrdering
  ): MemoizedFunction = {
    val tord: Ordering[DomainData] = LatisOrdering.partialToTotal(ordering)
    SortedMapFunction(SortedMap(samples: _*)(tord))
  }

}
