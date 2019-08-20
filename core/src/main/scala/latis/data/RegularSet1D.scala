package latis.data

import latis.metadata._
import latis.model._

/**
 * Define a one-dimensional DomainSet in terms of a start value,
 * increment, and number of samples. Values will be managed as Doubles.
 * The data values represent regularly spaced "instantaneous" domain data
 * as opposed to binned data.
 */
class RegularSet1D(
  start: Double, increment: Double, count: Int
) extends DomainSet with Serializable {
  //TODO: consider SortedSet, range
  //TODO: consider "search" to return SearchResult, InsertionPoint
  //TODO: require increment > 0 otherwise need to provide alternate ordering
  //TODO: use Long for the count?
  //TODO: optional model constructor arg?
  //TODO: "Regular" vs "Linear"?
  
  /**
   * Return the number of values in this DomainSet.
   * Override to simply return the count.
   */
  override def length = count
  
  /**
   * Return the Data values of this DomainSet as an ordered Sequence.
   */
  def elements: Seq[DomainData] = for {
    index <- (0 until count)
  } yield  DomainData(start + increment * index)

  /**
   * Get the domain data for the given index.
   * Override to compute the data value.
   */
  override def getElement(index: Int): Option[DomainData] = 
    if (index >= 0 && index < length)
      Some(DomainData(start + increment * index))
    else None
  
  /**
   * Return the index of the matching domain data value.
   * Return -1 if not within the coverage of this DomainSet.
   * Override to compute the index.
   */
  override def indexOf(data: DomainData): Int = {
    val index = data match {
      case DomainData(Number(x)) => 
        ((x - start)/increment).toInt //rounds down
    }
    if (index >= 0 && index < length) index
    else -1
  }
    
}

object RegularSet1D {
  
  def apply(start: Double, increment: Double, count: Int) =
    new RegularSet1D(start, increment, count)
  
  def fromExtents(min: Double, max: Double, increment: Double): RegularSet1D = {
    //TODO: ensure max > min, increment > 0
    val start = min
    val count = ((max - min) / increment).toInt //will round down
    new RegularSet1D(start, increment, count)
  }
    
  //TODO: from min, max, count?
}
