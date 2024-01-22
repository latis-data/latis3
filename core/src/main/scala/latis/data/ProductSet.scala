package latis.data

import latis.model.*

/**
 * A DomainSet that is a Cartesian product of individual DomainSets.
 */
case class ProductSet(sets: Seq[DomainSet]) extends DomainSet {
  //TODO: deal with count < 2
  //TODO: optimize getElement,...

  override def model: DataType = Tuple.fromSeq(sets.map(_.model)).fold(throw _, identity)

  override def length: Int = sets.map(_.length).product

  override def shape: Array[Int] = sets.toArray.flatMap(_.shape)

  /*
   * does this work for sets with rank > 1?
   * seems like it might
   */

  def elements: IndexedSeq[DomainData] = {

    def prod(as: Seq[DomainData], bs: Seq[DomainData]): Seq[DomainData] =
      for {
        a <- as
        b <- bs
      } yield a ++ b

    sets.map(_.elements).reduce(prod).toIndexedSeq
  }

}

object ProductSet {

  def apply(fisrtSet: DomainSet, otherSets: DomainSet*): ProductSet =
    ProductSet(fisrtSet +: otherSets)
}
