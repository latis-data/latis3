package latis.data

import scala.util.Try
import scala.util.Success
import scala.util.Failure
import cats.effect.IO
import fs2._

/**
 * A SampledFunction implemented with a 2D array.
 * The domain values are 0-based indices as Ints.
 */
case class ArrayFunction2D(array: Array[Array[RangeData]]) extends MemoizedFunction {
  
  def apply(d: DomainData): Stream[Pure, RangeData] = d match {
    //TODO: support any integral type
    //TODO: handle index out of bounds
    case DomainData(i: Int, j: Int) => Stream.emit(array(i)(j))
    case _ => ??? //IO.raiseError(new RuntimeException("Failed to evaluate ArrayFunction2D"))
  }
  
  def samples: Stream[Pure, Sample] = {
    val ss: Seq[Sample] = for {
      i <- 0 until array.length
      j <- 0 until array(0).length
    } yield (DomainData(i, j), array(i)(j))
    
    Stream.emits(ss)
  }
}
