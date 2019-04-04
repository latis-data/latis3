package latis.data

/**
 * A SampledFunction implemented with a 1D array.
 * The domain values are 0-based indices as Ints.
 */
case class ArrayFunction1D(array: Array[RangeData]) extends MemoizedFunction {
  
  override def apply(d: DomainData): Option[RangeData] = d match {
    //TODO: support any integral type
    //TODO: handle index out of bounds
    case DomainData(i: Int) => Option(array(i))
    case _ => ??? //new RuntimeException("Failed to evaluate ArrayFunction1D")
  }
  
  def samples: Seq[Sample] = 
    Seq.tabulate(array.length) {
      i => Sample(DomainData(i), array(i))
    }

}
//TODO: fromSeq? CanBuildFrom? See FunctionFactory