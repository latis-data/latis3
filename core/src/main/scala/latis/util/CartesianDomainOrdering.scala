package latis.util

import latis.data.Datum
import latis.data.DomainData

case class CartesianDomainOrdering(ords: Seq[PartialOrdering[Datum]])
    extends PartialOrdering[DomainData] {

  def tryCompare(dd1: DomainData, dd2: DomainData): Option[Int] = {
    def go(os: Seq[PartialOrdering[Datum]], ds1: List[Datum], ds2: List[Datum]): Option[Int] =
      if (os.isEmpty) Some(0) //every pair was equal
      else
        os.head.tryCompare(ds1.head, ds2.head) match {
          case Some(0) => //equiv, recurse
            go(os.tail, ds1.tail, ds2.tail)
          case tc => tc
        }

    if (ords.length != dd1.length || ords.length != dd2.length) None
    else go(ords, dd1, dd2)
  }

  def lteq(dd1: DomainData, dd2: DomainData): Boolean = {
    def go(os: Seq[PartialOrdering[Datum]], ds1: List[Datum], ds2: List[Datum]): Boolean =
      if (os.isEmpty) true                        //every pair was equal
      else if (os.head.equiv(ds1.head, ds2.head)) //equiv, recurse
        go(os.tail, ds1.tail, ds2.tail)
      else os.head.lteq(ds1.head, ds2.head)

    if (ords.length != dd1.length || ords.length != dd2.length) false
    else go(ords, dd1, dd2)
  }
}
