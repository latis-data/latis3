package latis.data

/**
 * Define an Ordering for DomainData.
 * This is used by the implicit SampleOrdering
 * defined in the package object.
 */
object DomainOrdering extends Ordering[DomainData] {
  def compare(a: DomainData, b: DomainData) = {
    if (a.data.length != b.data.length) {
      val msg = "Can't compare DomainData with different arity."
      throw new UnsupportedOperationException(msg)
    }
    else comparePairs(a.data zip b.data)(ScalarOrdering)
  }
  
  /**
   * Helper method to compare two sequences of DomainData recursively.
   * If the first pair matches, recursively test the next pair.
   */
  private def comparePairs[T](ps: Seq[(T, T)])(implicit ord: Ordering[T]): Int = 
    ps.toList match {
      case Nil => 0 //all pairs matched
      case head :: tail => ord.compare(head._1, head._2) match {
        case 0 => comparePairs(tail)  //recurse
        case c: Int => c
      }
    }
  
}
