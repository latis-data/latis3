package latis.data

/**
 * Define a SampledFunction in terms of a DomainSet and a sequence of RangeData.
 */
case class SetFunction(domainSet: DomainSet, rangeValues: Seq[RangeData]) extends MemoizedFunction {
  //TODO: assert same size, make this private? any value in case class?

  def samples: Seq[Sample] = (domainSet.elements.zip(rangeValues)).map {
    case (r, d) => Sample(r, d)
  }

  override def apply(value: DomainData): Option[RangeData] = {
    val index = domainSet.indexOf(value)
    if (index >= 0 && index < rangeValues.length) Some(rangeValues(index))
    else None
  }

}
