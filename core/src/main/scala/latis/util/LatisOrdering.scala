package latis.util

import latis.data._
import latis.model._

/**
 * Defines Orderings to support LaTiS.
 */
object LatisOrdering {

  /**
   * Returns the default PartialOrdering for Datum.
   * This is also provided via Scalar.ordering for basic Scalars
   * such that special Scalars can provide a different ordering.
   */
  def dataOrdering: PartialOrdering[Datum] = new PartialOrdering[Datum] {
    //TODO: should we match specific value types?
    //  avoid conversion to doubles
    import scala.math.Ordering._

    def tryCompare(x: Datum, y: Datum): Option[Int] = (x, y) match {
      case (Number(d1), Number(d2)) =>
        if (d1.isNaN || d2.isNaN) None
        else Some(Double.compare(d1, d2))
      case (Text(s1), Text(s2)) =>
        Some(String.compare(s1, s2))
      case _ => None
    }

    def lteq(x: Datum, y: Datum): Boolean = (x, y) match {
      case (Number(d1), Number(d2)) =>
        d1 <= d2 //Note, always false for NaNs
      case (Text(s1), Text(s2)) =>
        String.compare(s1, s2) <= 0
      case _ => false
    }
  }

  /**
   * Returns a PartialOrdering for data corresponding to the given list
   * of Scalars. This is intended to be used to provide an ordering for
   * DomainData.
   */
  def domainOrdering(scalars: List[Scalar]): PartialOrdering[DomainData] =
    new PartialOrdering[DomainData] {

      def tryCompare(dd1: DomainData, dd2: DomainData): Option[Int] = {
        def go(ss: List[Scalar], ds1: List[Datum], ds2: List[Datum]): Option[Int] = {
          if (ss.isEmpty) Some(0) //every pair was equal
          else ss.head.ordering.tryCompare(ds1.head, ds2.head) match {
            case Some(0) => //equiv, recurse
              go(ss.tail, ds1.tail, ds2.tail)
            case tc => tc
          }
        }

        if (scalars.length != dd1.length || scalars.length != dd2.length) None
        else go(scalars, dd1, dd2)
      }

      def lteq(dd1: DomainData, dd2: DomainData): Boolean = {
        def go(ss: List[Scalar], ds1: List[Datum], ds2: List[Datum]): Boolean = {
          if (ss.isEmpty) true //every pair was equal
          else if (ss.head.ordering.equiv(ds1.head, ds2.head)) //equiv, recurse
              go(ss.tail, ds1.tail, ds2.tail)
          else ss.head.ordering.lteq(ds1.head, ds2.head)
        }

        if (scalars.length != dd1.length || scalars.length != dd2.length) false
        else go(scalars, dd1, dd2)
      }
    }

  /**
   * Provides a PartialOrdering for Samples.
   */
  def sampleOrdering(function: Function): PartialOrdering[Sample] = {
    val dord = function match {
      case Function(d, _) => domainOrdering(d.getScalars)
    }
    new PartialOrdering[Sample] {
      def tryCompare(s1: Sample, s2: Sample): Option[Int] =
        dord.tryCompare(s1.domain, s2.domain)
      def lteq(s1: Sample, s2: Sample): Boolean =
        dord.lteq(s1.domain, s2.domain)
    }
  }

  /**
   * Turns a PartialOrdering into a total Ordering
   * by throwing an exception when it fails.
   */
  def partialToTotal[A](po: PartialOrdering[A]): Ordering[A] =
    (a1: A, a2: A) => po.tryCompare(a1, a2).getOrElse {
      throw LatisException("Ordering failed")
    }

}
