package latis.ops

import latis.data.Sample
import latis.model.DataType

/**
 * Defines a unary operation that keeps every nth Sample
 * where n is the stride. The stride can have more than one element,
 * one for each dimension of a Cartesian Dataset.
 */
case class Stride(stride: Seq[Int]) extends Filter {
  //TODO: support nD stride for Cartesian Datasets

  def predicate(model: DataType): Sample => Boolean = {
    var count = -1
    (_: Sample) => {
      count = count + 1
      if (count % stride.head == 0) true
      else false
    }
  }

}

object Stride {
  def apply(n: Int): Stride = Stride(Seq(n))
}
