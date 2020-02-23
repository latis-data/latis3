package latis.util

import latis.data.DomainData

object DefaultDomainOrdering extends PartialOrdering[DomainData] {
  def tryCompare(dd1: DomainData, dd2: DomainData): Option[Int] = {
    val ords = Seq.fill(dd1.length)(DefaultDatumOrdering)
    CartesianDomainOrdering(ords).tryCompare(dd1, dd2)
  }
  def lteq(dd1: DomainData, dd2: DomainData): Boolean = {
    val ords = Seq.fill(dd1.length)(DefaultDatumOrdering)
    CartesianDomainOrdering(ords).lteq(dd1, dd2)
  }
}
