package latis.data

import latis.metadata._
import latis.model._

/**
 * A DomainSet represents the data values that make up the domain
 * of a SampledFunction (specifically a SetFunction).
 */
trait DomainSet {

  /**
   * Provide the data type for the variables represented by this DomainSet.
   */
  def model: DataType

  /**
   * Return the number of dimensions covered by this DomainSet.
   */
  def rank: Int = shape.length //assumes Cartesian
  //TODO: arity?

  /**
   * Return an Array with the length of each dimension of this DomainSet.
   */
  def shape: Array[Int] = Array(length) //1D, non-Cartesian
  /*
   * TODO: consider Cartesian vs other topologies:
   *   e.g. 2D manifold in 3D space: rank 3, shape size 2
   */

  /**
   * Return the DomainData elements of this DomainSet
   * based on its ordering. Multi-dimensional DomainSets
   * will vary slowest in its first dimension.
   * This returns an IndexedSeq to ensure that this
   * collection of elements can be rapidly accessed by Index
   * and is presumably strict (not lazy or tied to side effects).
   */
  def elements: IndexedSeq[DomainData]

  /**
   * Return the number of elements in this DomainSet.
   */
  def length: Int = elements.length

  /**
   * Return the minimum extent of the coverage of this DomainSet.
   */
  def min: DomainData = elements(0)

  /**
   * Return the maximum extent of the coverage of this DomainSet.
   */
  def max: DomainData = elements(length - 1)

  //TODO: first, last, take,...?

  /**
   * Optionally return the DomainData element at the given index.
   * If the index is out of bounds, None will be returned.
   */
  def apply(index: Int): Option[DomainData] =
    if (isDefinedAt(index)) Option(elements(index))
    else None

  /**
   * Is this DomainSet defined at the given index.
   */
  def isDefinedAt(index: Int): Boolean =
    (index >= 0) && (index < length)

  /**
   * Return the index of the given DomainData element.
   * If it does not exist, return -1.
   */
  def indexOf(data: DomainData): Int = elements.indexOf(data)
  //TODO: "search" to return SearchResult, InsertionPoint
  //TODO: contains for exact match?

  /**
   * Does the coverage of this DomainSet include the given element.
   * Note that this does not guarantee a *matching* element.
   */
  def covers(data: DomainData)(implicit ord: Ordering[DomainData]): Boolean =
    ord.gteq(data, min) && ord.lt(data, max)
  //TODO: min/max may rule out many sets, e.g. polygon

  /**
   * Defines the string representation of a DomainSet as the
   * model it represents.
   */
  override def toString: String = model.toString
}

object DomainSet {

  /**
   * Defines a DomainSet with a 1D manifold (not Cartesian).
   * This assumes that the values are Doubles for the sake of the model.
   */
  def apply(_elements: IndexedSeq[DomainData]): DomainSet = new DomainSet {

    def elements: IndexedSeq[DomainData] = _elements

    /**
     * Returns the rank as determined by inspecting the first element.
     */
    override def rank: Int = elements.headOption match {
      case Some(dd) => dd.length
      case None     => ??? //TODO: empty DomainSet
    }

    /**
     * Defines a model assuming variables are Doubles.
     * TODO: determine from values
     */
    def model: DataType = {
      val scalars = for {
        i  <- (0 until rank)
        md = Metadata(s"_$i") + ("type" -> "double")
      } yield Scalar(md)
      Tuple(scalars)
    }
  }
}
