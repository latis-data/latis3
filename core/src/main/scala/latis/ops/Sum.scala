package latis.ops

import cats.syntax.all._

import latis.data._
import latis.model._
import latis.util.LatisException

/**
 * Defines an Aggregation that reduces the Samples of a Dataset
 * by summing each range variable resulting in a Scalar or Tuple.
 */
case class Sum() extends Aggregation {

  def aggregateFunction(model: DataType): Iterable[Sample] => Data = samples =>
    Data.fromSeq(samples.map(_.range).reduce(sum))

  private def sum(r1: RangeData, r2: RangeData): RangeData = {
    // Note that types should align since these are from Samples of the same dataset
    r1.zip(r2).map {
      case (Integer(i1), Integer(i2)) => Data.LongValue(i1 + i2)
      case (Number(n1), Number(n2))   => Data.DoubleValue(n1 + n2)
      case _ => Data.DoubleValue(Double.NaN)
    }
  }

  def applyToModel(model:DataType): Either[LatisException, DataType] = model match {
    case Function(_, s: Scalar) => s.asRight
    case Function(_, t: Tuple)  => t.asRight
    case Function(_, _: Function) => LatisException("Sum does not sum nested Functions").asLeft
    case dt: DataType => dt.asRight //bare Scalar or Tuple
  }

}

object Sum {

  def builder: OperationBuilder = (args: List[String]) => {
    if (args.nonEmpty) LatisException("Sum does not take arguments").asLeft
    else Sum().asRight
  }
}
