package latis.time

import latis.data._

/**
 * Provides an Ordering for formatted time strings.
 */
object TimeOrdering {

  def fromFormat(format: TimeFormat): PartialOrdering[Datum] =
    new PartialOrdering[Datum] {
      // Note, None if data don't match our format
      def tryCompare(x: Datum, y: Datum): Option[Int] = (x, y) match {
        case (Text(t1), Text(t2)) =>
          val cmp = for {
            v1 <- format.parse(t1)
            v2 <- format.parse(t2)
          } yield Option(v1.compare(v2))
          cmp.getOrElse(None)
        case _ => None
      }

      def lteq(x: Datum, y: Datum): Boolean = (x, y) match {
        case (Text(t1), Text(t2)) =>
          val cmp = for {
            v1 <- format.parse(t1)
            v2 <- format.parse(t2)
          } yield v1 <= v2
          cmp.getOrElse(false)
        case _ => false
      }
    }

}
