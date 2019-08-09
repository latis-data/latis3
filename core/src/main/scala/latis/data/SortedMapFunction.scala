package latis.data

import scala.collection.immutable.SortedMap
import latis.resample._

case class SortedMapFunction(sortedMap: SortedMap[DomainData, RangeData]) extends MemoizedFunction {
  
  def samples: Seq[Sample] = sortedMap.toSeq
  
  override def apply(
    value: DomainData, 
    interpolation: Interpolation = NoInterpolation(),
    extrapolation: Extrapolation = NoExtrapolation()
  ): Option[RangeData] = {
    sortedMap.get(value)
    //TODO: support interpolation
  }
  
  //TODO: optimize other methods
}

object SortedMapFunction extends FunctionFactory {
  
  def fromSamples(samples: Seq[Sample]): MemoizedFunction =
    SortedMapFunction(SortedMap(samples: _*))
}
