package latis.data

import scala.language.postfixOps
import latis.resample._

/**
 * A SampledFunction implemented with a 2D array.
 * The domain values are 0-based indices as Ints.
 */
case class ArrayFunction2D(array: Array[Array[RangeData]]) extends MemoizedFunction {
    
  override def apply(
    dd: DomainData, 
    interpolation: Interpolation = NoInterpolation(),
    extrapolation: Extrapolation = NoExtrapolation()
  ): Option[RangeData] = dd match {
    //TODO: support any integral type
    //TODO: handle index out of bounds
    case DomainData(i: Int, j: Int) => Option(array(i)(j))
    case _ => ??? //new RuntimeException("Failed to evaluate ArrayFunction2D")
  }
  
  def samples: Seq[Sample] = 
    Seq.tabulate(array.length, array(0).length) { 
      (i, j) => Sample(DomainData(i, j), array(i)(j)) 
    } flatten

}

object ArrayFunction2D extends FunctionFactory {
  // Assumes the samples are a function of index with no gaps
  // so we can determine the shape of the array from the domain 
  // of the last sample
  def fromSamples(samples: Seq[Sample]): MemoizedFunction = samples match {
    case Seq() => ???              // TODO: figure out how to handle error
    case _ =>
      samples.last.domain match {
        case x +: xs => 
          val y = xs.head
          val xSize: Int = x.toString.toInt
          val ySize: Int = y.toString.toInt
          val array = Array.ofDim[RangeData](xSize + 1, ySize + 1)
          for {
            i <- 0 until xSize
            j <- 0 until ySize
          } array(i)(j) = samples(i + j * xSize).range
          ArrayFunction2D(array)
        case _ => ???              // TODO: figure out how to handle error
      }
  }
    
}
  