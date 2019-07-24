package latis.data

/**
 * Define a two-dimensional Cartesian DomainSet in terms of scales and offsets.
 */
case class LinearSet2D(
  xScale: Double, xOffset: Double, nx: Int,
  yScale: Double, yOffset: Double, ny: Int
) extends DomainSet {

  override def length: Int = nx * ny
  
  /**
   * Provide a Seq of DomainData computed from scale and offset.
   */
  def elements: Seq[DomainData] = for {
    ix <- (0 until nx)
    iy <- (0 until ny)
  } yield DomainData(
    xScale * ix + xOffset,
    yScale * iy + yOffset
  )
  
  /**
   * Find the index of the matching DomainData.
   * Note that this rounds to the nearest index (0.5 always rounds up)
   * to provide bin-centered semantics.
   */
  override def indexOf(data: DomainData): Int = data match {
    //Note, adding the 0.5 then floor effectively rounds to the nearest index.
    //We could use "round" but it's not clear if rounding up at 0.5 is guaranteed.
    case DomainData(x: Double, y: Double) => 
      val ix = Math.floor((x - xOffset)/xScale + 0.5).toInt
      val iy = Math.floor((y - yOffset)/yScale + 0.5).toInt
      // Don't extrapolate. Return None if out of bounds.
      if (ix >= 0 && ix < nx && iy >= 0 && iy < ny) ix * ny + iy
      else -1 //out of range
    case _ => ??? //TODO: error, invalid input
  }
}
