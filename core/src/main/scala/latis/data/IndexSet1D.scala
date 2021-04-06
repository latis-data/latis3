package latis.data

import latis.metadata._
import latis.model._
import latis.util.LatisException

//Experimental Int version of LinearSet
/**
 * Define a one-dimensional DomainSet in terms of a start value,
 * increment, and number of samples. Values will be managed as Doubles.
 * The data values represent regularly spaced "instantaneous" domain data
 * as opposed to bins.
 */
case class IndexSet1D(start: Int, increment: Int, count: Int) extends DomainSet with Serializable {
  //TODO: require increment > 0 otherwise need to provide alternate ordering
  //TODO: optional model constructor arg?

  /**
   * Define the model of this DomainSet assuming double value types
   * and a 1-based "_#" naming scheme for variable identifiers.
   */
  def model: DataType =
    Scalar(Metadata("id" -> "_1", "type" -> "int"))

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
      case DomainData(Index(x)) =>
        val i = (x - start) / increment
        // Must be integral for a match to an existing element
        if (i == Math.floor(i.toDouble)) i.toInt
        else -1
      case _ =>
        val msg = s"Invalid value for one-dimensional domain IndexSet: $data"
        throw LatisException(msg)
    }
    if (isDefinedAt(index)) index
    else -1
  }
}
