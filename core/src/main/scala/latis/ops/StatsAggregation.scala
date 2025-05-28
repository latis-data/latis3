package latis.ops

import cats.effect.*
import cats.syntax.all.*
import fs2.*

import latis.data.*
import latis.data.Data.*
import latis.model.*
import latis.util.Identifier.id
import latis.util.LatisException

/**
 * Compute statistics for the Samples of a Dataset.
 *
 * This expects a Function with a single numeric range variable.
 * This aggregation will reduce all Samples to a single Tuple
 * containing the mean, min, max, and count statistics for that
 * variable. If the sample stream is empty, NullData will be used.
 */
class StatsAggregation() extends Aggregation2 {
  //TODO: preserve integers
  //TODO: allow text and keep min/max (lexically), and count but have mean be NaN?

  def aggregateFunction(model: DataType): Stream[IO, Sample] => IO[Data] =
    samples => samples.fold(Acc()) {
      case (Acc(sum, min, max, cnt), Sample(_, RangeData(Number(d)))) =>
        Acc(sum + d, Math.min(min, d), Math.max(max, d), cnt + 1)
    }.map {
      case Acc(sum, min, max, cnt) =>
        if (cnt == 0) NullData
        else TupleData(
          DoubleValue(sum / cnt),
          DoubleValue(min),
          DoubleValue(max),
          LongValue(cnt)
        )
    }.compile.last.map(_.get) //guaranteed to have exactly one entry

  def applyToModel(model: DataType): Either[LatisException, DataType] = model match {
    case Function(_, s: Scalar) => s.valueType match {
      case _: NumericType => scalarToTuple(s)
      case _ => LatisException("StatsAggregation expects numeric data").asLeft
    }
    case _ => LatisException("StatsAggregation expects a single range variable").asLeft
  }

  private def scalarToTuple(scalar: Scalar): Either[LatisException, Tuple] =
    //TODO: preserve some metadata, units...
    Tuple.fromElements(scalar.id,
      Scalar(id"mean",  DoubleValueType),
      Scalar(id"min",   DoubleValueType),
      Scalar(id"max",   DoubleValueType),
      Scalar(id"count", LongValueType)
    )
}

object StatsAggregation {

  def builder: OperationBuilder = (args: List[String]) => 
    Either.cond(
      args.isEmpty,
      StatsAggregation(),
      LatisException("StatsAggregation take no arguments")
    )
}

// Local case class for accumulating stats
private case class Acc(
  sum: Double = 0.0,
  min: Double = Double.MaxValue,
  max: Double = Double.MinValue,
  count: Long = 0L
)
