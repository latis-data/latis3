package latis.util

import latis.data.*
import latis.model.*

/**
 * Defines Orderings to support LaTiS.
 */
object LatisOrdering {
  //TODO: use explicit ordering classes

  /**
   * Defines the default partial ordering for Samples.
   */
  def defaultSampleOrdering: PartialOrdering[Sample] = {
    new PartialOrdering[Sample] {
      def tryCompare(s1: Sample, s2: Sample): Option[Int] =
        DefaultDomainOrdering.tryCompare(s1.domain, s2.domain)
      def lteq(s1: Sample, s2: Sample): Boolean =
        DefaultDomainOrdering.lteq(s1.domain, s2.domain)
    }
  }

  /**
   * Defines a PartialOrdering for Samples based on the function type.
   */
  def sampleOrdering(function: Function): PartialOrdering[Sample] = {
    val dord = CartesianDomainOrdering(function.domain.getScalars.map(_.ordering))
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
    (a1: A, a2: A) =>
      po.tryCompare(a1, a2).getOrElse {
        val msg = s"Ordering failed for $a1, $a2"
        throw LatisException(msg)
      }

}
