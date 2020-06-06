package latis.util

import scala.math.Ordering._

import latis.data._

object DefaultDatumOrdering extends PartialOrdering[Datum] {
  /*
  TODO: should we match specific value types? avoid conversion to doubles
        but would require exact type match
    do we need to keep unorderable Datums out of domain?
      only boolean and binary?
      default ord currently based on match to Number or Text
      null put at end but not good
        required for spark groupBy?
      require only non-nullable vars in domain?
      Orderable trait?
   */

  def tryCompare(x: Datum, y: Datum): Option[Int] = (x, y) match {
    case (Number(d1), Number(d2)) =>
      if (d1.isNaN || d2.isNaN) None
      else Some(Ordering[Double].compare(d1, d2))
    case (Text(s1), Text(s2)) =>
      Some(String.compare(s1, s2))
    // Make NullDatum larger than any other
    // Unlike NaN, allow NullDatum == NullDatum
    case (NullDatum, NullDatum) => Some(0)
    case (NullDatum, _)         => Some(1)
    case (_, NullDatum)         => Some(-1)
    case _                      => None
  }

  def lteq(x: Datum, y: Datum): Boolean = (x, y) match {
    case (Number(d1), Number(d2)) =>
      d1 <= d2 //Note, always false for NaNs
    case (Text(s1), Text(s2)) =>
      String.compare(s1, s2) <= 0
    case _ => false
  }
}
