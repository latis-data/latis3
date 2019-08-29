package latis.data

/**
 * Define a two-dimensional linear (regularly spaced) domain set 
 * where each domain value represents a bin center. 
 * The bins have the same size and are contiguous.
 */
case class BinSet2D(set1: BinSet1D, set2: BinSet1D) extends LinearSet2D(set1, set2)

object BinSet2D {
  
  /**
   * Define a 2-dimensional domain set with regular sized bins from the
   * minimum and maximum extents of its coverage and a suggested total
   * bin count. The resulting number of bins along each dimension will
   * be computed to preserve the aspect ratio of the given bounding box.
   * As a result, the final size may differ from the request.
   * Note that this assumes that each dimension has the same units when 
   * computing the aspect ratio.
   */
  def fromExtents(min: (Double, Double), max: (Double, Double), count: Int) = {
    val d1 = max._1 - min._1
    val d2 = max._2 - min._2
    val n1: Int = Math.round(Math.sqrt(d1 * count / d2)).toInt
    val n2: Int = Math.round(count.toFloat / n1)
    
    val set1 = BinSet1D.fromExtents(min._1, max._1, n1)
    val set2 = BinSet1D.fromExtents(min._2, max._2, n2)
    
    BinSet2D(set1, set2)
  }
}
