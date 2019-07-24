package latis.data

import scala.collection.Searching._
import fs2._
import cats.effect._
import latis.resample._

/**
 * Manage two-dimensional Function Data as columnar arrays.
 * For evaluation, this uses a binary search on the domain arrays
 * to get the indices into the range array.
 * This assumes a Cartesian product domain set.
 */
case class IndexedFunction2D(as: Array[Any], bs: Array[Any], vs: Array[Array[Any]]) extends MemoizedFunction {
  //TODO: consider smart constructor: IndexedFunction(ds: Array[Data]*)(vs: Array[Data])
  //TODO: assert that sizes align
  //TODO: should range values be RangeData instead of Any so we can have multiple variables?
  
  override def apply(
    dd: DomainData, 
    interpolation: Interpolation = NoInterpolation(),
    extrapolation: Extrapolation = NoExtrapolation()
  ): Option[RangeData] = dd match {
    case DomainData(a, b) =>
      val ia = as.search(a)(ScalarOrdering) match {
        case Found(i) => i
        case InsertionPoint(i) => ??? //TODO: interpolate
      }
      val ib = bs.search(b)(ScalarOrdering) match {
        case Found(i) => i
        case InsertionPoint(i) => ??? //TODO: interpolate
      }
      Option(RangeData(vs(ia)(ib)))
    case _ => ??? //TODO: error
  }
  
  def samples: Seq[Sample] = {
    for {
      ia <- 0 until as.length
      ib <- 0 until bs.length
    } yield Sample(DomainData(as(ia), bs(ib)), RangeData(vs(ia)(ib)))
  }
}

//TODO: fromSeq? CanBuildFrom? See FunctionFactory
