package latis.data

import scala.collection.immutable.SortedMap

import latis.util.LatisException

case class SortedMapFunction(sortedMap: SortedMap[DomainData, RangeData]) extends MemoizedFunction {

  def sampleSeq: Seq[Sample] = sortedMap.toSeq

  //override def apply(value: DomainData): Either[LatisException, RangeData] =
  //  sortedMap.get(value) match {
  //    case Some(r) => Right(r)
  //    case None =>
  //      val msg = s"No sample found matching $value"
  //      Left(LatisException(msg))
  //  }
  //TODO: support interpolation

  //TODO: optimize other methods
}

object SortedMapFunction { //extends FunctionFactory {

  def fromSamples(samples: Seq[Sample])(implicit ordering: Ordering[DomainData]): MemoizedFunction =
    SortedMapFunction(SortedMap(samples: _*)(ordering))

}
