package latis.data

import latis.util.DefaultDomainOrdering
import latis.util.LatisException

/**
 * Defines a SampledFunction implemented with a 1D array.
 * The domain values are 0-based indices as Ints.
 */
case class ArrayFunction1D(array: Array[RangeData]) extends MemoizedFunction {

  /**
   * Defines the ordering for the domain indices.
   */
  def ordering: Option[PartialOrdering[DomainData]] = Some(DefaultDomainOrdering)

  override def apply(data: DomainData): Either[LatisException, RangeData] = data match {
    case DomainData(Index(i)) =>
      array.lift(i) match {
        case Some(r) => Right(r)
        case None =>
          val msg = s"No sample found matching $data"
          Left(LatisException(msg))
      }
    case _ => Left(LatisException(s"Invalid evaluation value for ArrayFunction1D: $data"))
  }

  def sampleSeq: Seq[Sample] =
    Seq.tabulate(array.length) { i =>
      Sample(DomainData(i), RangeData(array(i)))
    }

}
//TODO: fromSeq? See FunctionFactory
