package latis.util

import scala.math.Ordering._

import latis.data._

object DefaultDatumOrdering extends PartialOrdering[Datum] {
  /*
  TODO: should we match specific value types? avoid conversion to doubles
        but would require exact type match
    do we need to keep unorderable Datums out of domain?
      only binary?
      default ord currently based on match to Number or Text or BooleanValue
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
    case (b1: Data.BooleanValue, b2: Data.BooleanValue) =>
      Some(Boolean.compare(b1.value, b2.value))
    case _ => None
  }

  def lteq(x: Datum, y: Datum): Boolean = (x, y) match {
    case (Number(d1), Number(d2)) =>
      d1 <= d2 //Note, always false for NaNs
    case (Text(s1), Text(s2)) =>
      String.compare(s1, s2) <= 0
    case (b1: Data.BooleanValue, b2: Data.BooleanValue) =>
      Boolean.compare(b1.value, b2.value) <= 0
    case _ => false
  }
}
