package latis.data

import latis.util.LatisException

/**
 * Define a SampledFunction in terms of a DomainSet and a sequence of RangeData.
 */
case class SetFunction(domainSet: DomainSet, rangeValues: Seq[RangeData]) extends MemoizedFunction {
  //TODO: assert same size, make this private? any value in case class?

  def sampleSeq: Seq[Sample] = (domainSet.elements.zip(rangeValues)).map {
    case (r, d) => Sample(r, d)
  }

  //override def apply(value: TupleData): Either[LatisException, TupleData] = {
  //  val index = domainSet.indexOf(value)
  //  if (index >= 0 && index < rangeValues.length) Right(rangeValues(index))
  //  else {
  //    val msg = s"No sample found matching $value"
  //    Left(LatisException(msg))
  //  }
  //}

}
