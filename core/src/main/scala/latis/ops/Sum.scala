package latis.ops

import cats.syntax.all.*

import latis.data.*
import latis.model.*
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
    case s: Scalar                => updateType(s)
    case t: Tuple                 => updateTupleTypes(t)
    case Function(_, s: Scalar)   => updateType(s)
    case Function(_, t: Tuple)    => updateTupleTypes(t)
    case Function(_, _: Function) => LatisException("Sum does not support nested Functions").asLeft
  }

  private def updateType(scalar: Scalar): Either[LatisException, DataType] = {
    scalar.valueType match {
      case _: IntegralType => Scalar.fromMetadata(scalar.metadata + ("type", "long"))
      case _: NumericType  => Scalar.fromMetadata(scalar.metadata + ("type", "double"))
      case _               =>
        Scalar.fromMetadata(
          scalar.metadata + ("type", "double") + ("fillValue", "NaN") - "missingValue"
        )
    }
  }

  private def updateTupleTypes(tuple: Tuple): Either[LatisException, Tuple] =
    tuple.flatElements.traverse {
      case s: Scalar => updateType(s)
      case _         => LatisException("Sum does not support nested Functions").asLeft
    }.flatMap(Tuple.fromSeq)

}

object Sum {

  def builder: OperationBuilder = (args: List[String]) => {
    if (args.nonEmpty) LatisException("Sum does not take arguments").asLeft
    else Sum().asRight
  }
}
