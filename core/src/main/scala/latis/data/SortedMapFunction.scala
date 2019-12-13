package latis.data

import scala.collection.immutable.SortedMap

import latis.model.Scalar
import latis.util.LatisOrdering

case class SortedMapFunction(sortedMap: SortedMap[DomainData, RangeData])(
  implicit ordering: Ordering[DomainData]
) extends MemoizedFunction {

  def samples: Seq[Sample] = sortedMap.toSeq

  override def apply(value: DomainData): Option[RangeData] =
    sortedMap.get(value)
  //TODO: support interpolation

  //TODO: optimize other methods
}

object SortedMapFunction { //extends FunctionFactory {

  def fromSamples(samples: Seq[Sample])(implicit ordering: Ordering[DomainData]): MemoizedFunction =
    SortedMapFunction(SortedMap(samples: _*)(ordering))

}
