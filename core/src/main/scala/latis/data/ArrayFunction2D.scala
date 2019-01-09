package latis.data

import scala.language.postfixOps

/**
 * A SampledFunction implemented with a 2D array.
 * The domain values are 0-based indices as Ints.
 */
case class ArrayFunction2D(array: Array[Array[RangeData]]) extends MemoizedFunction {
  
  override def apply(d: DomainData): ArrayFunction2D = d match {
    //TODO: support any integral type
    //TODO: handle index out of bounds
    case DomainData(i: Int, j: Int) => ArrayFunction2D(Array(Array(array(i)(j))))
    case _ => ??? //new RuntimeException("Failed to evaluate ArrayFunction2D")
  }
  
  def samples: Seq[Sample] = 
    Seq.tabulate(array.length, array(0).length) { 
      (i, j) => Sample(DomainData(i, j), array(i)(j)) 
    } flatten

}
//TODO: fromSeq? CanBuildFrom? See FunctionFactory