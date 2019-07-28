package latis.data

import scala.collection.Searching._
import latis.resample._

/**
 * Manage one-dimensional SampledFunction Data as columnar sequences.
 * For evaluation, this uses a binary search on the domain values
 * to get the index into the range values.
 */
case class IndexedFunction1D(xs: Seq[OrderedData], vs: Seq[RangeData]) extends IndexedFunction {
  //TODO: assert that sizes are consistent
  //Note, using Seq instead of invariant Array to get variance
  //TODO: prevent diff types of OrderedData, e.g. mixing NumberData and TextData, or IntData and DoubleData
  // [T <: OrderedData]
  
  override def apply(
    dd: DomainData, 
    interpolation: Interpolation = NoInterpolation(),
    extrapolation: Extrapolation = NoExtrapolation()
  ): Option[RangeData] = dd match {
    case DomainData(d) =>
      searchDomain(xs, d) match {
        case Found(i) => Option(vs(i))
        case InsertionPoint(i) => ??? //TODO: interpolate
      }
  }
  
  /**
   * Provide a sequence of samples to fulfill the MemoizedFunction trait.
   */
  def samples: Seq[Sample] =
    (xs zip vs) map { case (x, v) => Sample(DomainData(x), v) }

}
