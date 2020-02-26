package latis.data

import latis.util.LatisException

/**
 * Defines a SampledFunction in terms of a DomainSet and a sequence of RangeData.
 * The domain set provides indices into the range sequence which otherwise has
 * no structure.
 */
case class SetFunction(domainSet: DomainSet, rangeValues: Seq[RangeData]) extends MemoizedFunction {
  //TODO: assert same size, make this private? any value in case class?
  //  require smart constructor returning either

  /**
   * Provides the ordering for this SampledFunction from the
   * DomainSet that implements it.
   */
  def ordering: Option[PartialOrdering[DomainData]] = domainSet.ordering

  def sampleSeq: Seq[Sample] = (domainSet.elements.zip(rangeValues)).map {
    case (r, d) => Sample(r, d)
  }

  override def apply(data: DomainData): Either[LatisException, RangeData] = {
    //TODO: support interpolation
    val index = domainSet.indexOf(data)
    if (index >= 0 && index < rangeValues.length) Right(rangeValues(index))
    else {
      val msg = s"No sample found matching $data"
      Left(LatisException(msg))
    }
  }

}
