package latis.data

import latis.model._
import latis.metadata.Metadata

/**
 * Define a two-dimensional Cartesian DomainSet with regularly spaced elements.
 */
class RegularSet2D(set1: RegularSet1D, set2: RegularSet1D) extends DomainSet with Serializable {
  //TODO: ProductSet: cartesian but not nec. linear
  
  override def length = set1.length * set2.length
  
  override def shape: Array[Int] = Array(set1.length, set2.length)
 
  def elements: Seq[DomainData] = for {
    dd1 <- set1.elements
    dd2 <- set2.elements
  } yield DomainData(dd1 ++ dd2: _*)
  
  override def getElement(index: Int): Option[DomainData] = {
    if (index >= 0 && index < length) {
      val i1: Int = index / set2.length
      val i2: Int = index - (i1 * set2.length)
      for {
        dd1 <- set1.getElement(i1)
        dd2 <- set2.getElement(i2)
      } yield DomainData(dd1 ++ dd2: _*)
    } else None
  }

  override def indexOf(data: DomainData): Int = data match {
    case DomainData(d1, d2) => 
      val i1 = set1.indexOf(DomainData(d1))
      val i2 = set2.indexOf(DomainData(d2))
      if (i1 >= 0 && i1 < set1.length && 
          i2 >= 0 && i2 < set2.length)
        i1 * set2.length + i2
      else -1
  }
  
}

//case class RegularSet2D0(
//  xScale: Double, xOffset: Double, nx: Int,
//  yScale: Double, yOffset: Double, ny: Int
//) //extends DomainSet {
////TODO: use start, stride semantics; any performance implications?
//  //TODO: build from 2 1D sets?
//  
//  //TODO: construct with model so users can name things, support diff types?
//  def model: DataType = Tuple(
//    Scalar(Metadata("id" -> "_1", "type" -> "double")),
//    Scalar(Metadata("id" -> "_2", "type" -> "double"))
//  )
//  
//  override def getElement(index: Int): Option[DomainData] = {
//    // index = ny * ix + iy
//    val ix: Int = index / ny
//    val iy: Int =  index - (ix * ny)
//    if (ix >= 0 && ix < nx && iy >= 0 && iy < ny) {
//      val x = ix * xScale + xOffset
//      val y = iy * yScale + yOffset
//      Some(DomainData(x, y))
//    }
//    else None
//  }
//  
//  override def length: Int = nx * ny
//  
//  /**
//   * Provide a Seq of DomainData computed from scale and offset.
//   */
//  def elements: Seq[DomainData] = for {
//    ix <- (0 until nx)
//    iy <- (0 until ny)
//  } yield DomainData(
//    xScale * ix + xOffset,
//    yScale * iy + yOffset
//  )
//  
//  /**
//   * Find the index of the matching DomainData.
//   * Note that this rounds to the nearest index (0.5 always rounds up)
//   * to provide bin-centered semantics.
//   */
//  override def indexOf(data: DomainData): Int = data match {
//    //Note, adding the 0.5 then floor effectively rounds to the nearest index.
//    //We could use "round" but it's not clear if rounding up at 0.5 is guaranteed.
//    case DomainData(Number(x), Number(y)) => 
//      val ix = Math.floor((x - xOffset)/xScale + 0.5).toInt
//      val iy = Math.floor((y - yOffset)/yScale + 0.5).toInt
//      // Don't extrapolate. Return None if out of bounds.
//      if (ix >= 0 && ix < nx && iy >= 0 && iy < ny) ix * ny + iy
//      else -1 //out of range
//    case _ => ??? //TODO: error, invalid input
//  }
//}
