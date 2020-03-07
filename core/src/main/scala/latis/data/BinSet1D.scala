package latis.data

import latis.util.LatisException

/**
 * Define a one-dimensional linear (regularly spaced) domain set
 * where each domain value represents the start of a bin.
 * The bins have the same size and are contiguous.
 * This behaves like a LinearSet except for indexOf which will
 * match any value within a bin (inclusive at the beginning,
 * exclusive at the end).
 */
class BinSet1D(start: Double, increment: Double, count: Int)
  extends LinearSet1D(start, increment, count)
  with Serializable {

  /**
   * Override to match any value that falls within a bin.
   */
  override def indexOf(data: DomainData): Int = {
    val index = data match {
      case DomainData(Number(x)) =>
        ((x - start) / increment).toInt
      case _ =>
        val msg = s"Invalid value for one-dimensional domain set: $data"
        throw LatisException(msg)
    }
    if (index >= 0 && index < length) index
    else -1
  }
}

object BinSet1D {

  def apply(start: Double, increment: Double, count: Int): BinSet1D =
    new BinSet1D(start, increment, count)

  def fromExtents(min: Double, max: Double, count: Int): BinSet1D = {
    //TODO: ensure max > min
    //TODO: should extents be total coverage or midpoints? latter?
    val start     = min
    val increment = ((max - min) / count) //bin semantics
    new BinSet1D(start, increment, count)
  }

  def fromExtents(min: Double, max: Double, increment: Double): BinSet1D = {
    //TODO: ensure max > min
    val count = ((max - min) / increment).toFloat.toInt //will round down, toFloat cleans up precision
    new BinSet1D(min, increment, count)
  }

}
