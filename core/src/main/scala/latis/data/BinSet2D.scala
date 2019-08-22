package latis.data

case class BinSet2D(set1: BinSet1D, set2: BinSet1D) extends RegularSet2D(set1, set2) with Serializable {
  
  /*
   * TODO: what do we need to override?
   * min, max
   * 
   */
}

object BinSet2D {
  
  def fromExtents(min: (Double, Double), max: (Double, Double), count: Int) = {
   
    // Compute x and y dimension size from total requested pixel count
    // preserving the aspect ratio (assuming lat degrees = lon degrees).
    //val nx: Int = Math.round(Math.sqrt((lon2-lon1) * count / (lat2-lat1))).toInt
    val d1 = max._1 - min._1
    val d2 = max._2 - min._2
    val n1: Int = Math.round(Math.sqrt(d1 * count / d2)).toInt
    val n2: Int = Math.round(count.toFloat / n1)
    
    //TODO: n-1?
    val set1 = BinSet1D(min._1, d1/n1, n1)
    val set2 = BinSet1D(min._2, d2/n2, n2)
    
    BinSet2D(set1, set2)
  }
}