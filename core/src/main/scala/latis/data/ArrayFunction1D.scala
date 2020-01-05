package latis.data

import latis.util.LatisException

/**
 * A SampledFunction implemented with a 1D array.
 * The domain values are 0-based indices as Ints.
 */
case class ArrayFunction1D(array: Array[RangeData]) extends MemoizedFunction {

  override def apply(value: DomainData): Either[LatisException, RangeData] = value match {
    case DomainData(Index(i)) => array.lift(i) match {
      case Some(r) => Right(r)
      case None =>
        val msg = s"No sample found matching $value"
        Left(LatisException(msg))
    }
    case _ => Left(LatisException(s"Invalid evaluation value for ArrayFunction1D: $value"))
  }

  def sampleSeq: Seq[Sample] =
    Seq.tabulate(array.length) { i =>
      Sample(DomainData(i), array(i))
    }

}
//TODO: fromSeq? See FunctionFactory
