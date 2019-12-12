package latis.data

import scala.collection.immutable.SortedMap

import latis.model.Scalar
import latis.util.LatisOrdering

case class SortedMapFunction(sortedMap: SortedMap[DomainData, RangeData]) extends MemoizedFunction {

  def samples: Seq[Sample] = sortedMap.toSeq

  override def apply(value: DomainData): Option[RangeData] =
    sortedMap.get(value)
  //TODO: support interpolation

  //TODO: optimize other methods
}

//object SortedMapFunction extends FunctionFactory {
//
//  def fromSamples(samples: Seq[Sample], ss: List[Scalar]): MemoizedFunction = {
//    val ordering: Ordering[DomainData] = LatisOrdering.partialToTotal(LatisOrdering.domainOrdering(ss))
//    SortedMapFunction(SortedMap(samples: _*)(ordering))
//  }
//}
