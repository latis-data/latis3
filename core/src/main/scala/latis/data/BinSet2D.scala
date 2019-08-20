package latis.data

case class BinSet2D(set1: BinSet1D, set2: BinSet1D) extends RegularSet2D(set1, set2) with Serializable {
  
  /*
   * TODO: what do we need to override?
   * min, max
   * 
   */
}