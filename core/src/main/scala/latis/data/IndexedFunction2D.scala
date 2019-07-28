package latis.data

import scala.collection.Searching._
import latis.resample._

/**
 * Manage two-dimensional Cartesian SampledFunction Data as columnar arrays.
 * The Cartesian nature allows the domain values for each dimension to be
 * managed separately.
 * For evaluation, this uses a binary search on the domain values
 * to get the indices into the range values.
 */
case class IndexedFunction2D(xs: Seq[OrderedData], ys: Seq[OrderedData], vs: Seq[Seq[RangeData]]) extends IndexedFunction {
  //TODO: assert that sizes are consistent
  
  override def apply(
    dd: DomainData, 
    interpolation: Interpolation = NoInterpolation(),
    extrapolation: Extrapolation = NoExtrapolation()
  ): Option[RangeData] = dd match {
    case DomainData(x, y) =>
      (searchDomain(xs, x), searchDomain(ys, y)) match {
        case (Found(i), Found(j)) => Option(vs(i)(j))
        case _ => ??? //TODO: interpolate
      }
    case _ => ??? //TODO: error, invalid type
  }
  
  /**
   * Provide a sequence of samples to fulfill the MemoizedFunction trait.
   */
  def samples: Seq[Sample] = {
    for {
      ia <- 0 until xs.length
      ib <- 0 until ys.length
    } yield Sample(DomainData(xs(ia), ys(ib)), vs(ia)(ib))
  }
}
