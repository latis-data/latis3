package latis.data

import latis.model._
import latis.util.Identifier._
import latis.util.LatisException

/**
 * Define a one-dimensional DomainSet in terms of a start value,
 * increment, and number of samples. Values will be managed as Doubles.
 * The data values represent regularly spaced "instantaneous" domain data
 * as opposed to bins.
 */
class LinearSet1D(start: Double, increment: Double, count: Int)
  extends DomainSet
  with Serializable {
  //TODO: require increment > 0 otherwise need to provide alternate ordering
  //TODO: optional model constructor arg?

  /**
   * Define the model of this DomainSet assuming double value types
   * and a 1-based "_#" naming scheme for variable identifiers.
   */
  def model: DataType = Scalar(id"_1", DoubleValueType)

  /**
   * Return the number of values in this DomainSet.
   * Override to simply return the count.
   */
  override def length: Int = count

  override def min: DomainData = DomainData(start)
  override def max: DomainData = DomainData(start + increment * (count - 1))

  /**
   * Return the Data values of this DomainSet as an ordered Sequence.
   */
  def elements: IndexedSeq[DomainData] = for {
    index <- (0 until count)
  } yield DomainData(start + increment * index)

  /**
   * Get the domain data for the given index.
   * Override to compute the data value.
   */
  override def apply(index: Int): Option[DomainData] =
    if (isDefinedAt(index)) Some(DomainData(start + increment * index))
    else None

  /**
   * Return the index of the matching domain data value.
   * Return -1 if not contained in this DomainSet.
   * Override to compute the index.
   */
  override def indexOf(data: DomainData): Int = {
    val index: Int = data match {
      case DomainData(Number(x)) =>
        val i = (x - start) / increment
        // Must be integral for a match to an existing element
        if (i == Math.floor(i)) i.toInt //TODO: precision issues?
        else -1
      case _ =>
        val msg = s"Invalid value for one-dimensional domain set: $data"
        throw LatisException(msg)
    }
    if (isDefinedAt(index)) index
    else -1
  }

}

object LinearSet1D {

  def apply(start: Double, increment: Double, count: Int) =
    new LinearSet1D(start, increment, count)

  def fromExtents(min: Double, max: Double, count: Int): LinearSet1D = {
    //TODO: ensure max > min
    val start = min
    // Note, the "-1" from the count provides "instantaneous" semantics
    //   (i.e. not binned)
    val increment = ((max - min) / (count - 1))
    new LinearSet1D(start, increment, count)
  }

  def fromExtents(min: Double, max: Double, increment: Double): LinearSet1D = {
    //TODO: ensure max > min, increment > 0
    val start = min
    val count = ((max - min) / increment).toInt //will round down
    new LinearSet1D(start, increment, count)
  }
}
