package latis.data

import scala.collection.Searching._
import fs2._
import cats.effect.IO

/**
 * Manage one-dimensional Function Data as columnar arrays.
 * For evaluation, this uses a binary search on the domain array
 * to get the index into the range array.
 */
case class IndexedFunction1D(as: Array[Any], vs: Array[Any]) extends MemoizedFunction {
  //TODO: consider computable index (regular spacing)
  //TODO: consider coordinate system function composition
  //TODO: combine with ArrayFunction?
  
  override def apply(dd: DomainData): Option[RangeData] = dd match {
    case DomainData(d) =>
      as.search(d)(ScalarOrdering) match {
        case Found(i) => Option(RangeData(vs(i)))
        case InsertionPoint(i) => ??? //TODO: interpolate
      }
  }
  
  def samples: Seq[Sample] =
    (as zip vs) map { case (a, v) => Sample(DomainData(a), RangeData(v)) }

}

//TODO: fromSeq? CanBuildFrom? See FunctionFactory
