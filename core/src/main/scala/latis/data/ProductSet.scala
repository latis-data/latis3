package latis.data

import latis.model._

/**
 * A DomainSet that is a Cartesian product of individual DomainSets.
 */
case class ProductSet(sets: DomainSet*) extends DomainSet {
  //TODO: deal with count < 2
  //TODO: optimize getElement,...
  
  override def model: DataType = Tuple(sets.map(_.model): _*)

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
