package latis.ops

import cats.effect.IO
import cats.syntax.all.*
import fs2.*

import latis.data.*
import latis.data.Data.DoubleValue
import latis.model.*
import latis.util.LatisException

/**
 * Group Samples by binning the domain variable for a target number of bins.
 *
 * This requires a Function with a one-dimensional, numeric domain with
 * coverage metadata.
 * The optional [[aggregation]] operation will reduce the binned Samples
 * into a single Data object. By default, this will be a nested Function.
 */
class GroupByBinCount private (
  count: Int,
  aggregation: Aggregation2 = DefaultAggregation2()
) extends StreamOperation {
  
  //TODO: memoize?
  private def groupByBinWidth(model: DataType): Either[LatisException, GroupByBinWidth] = {
    (model match {
      case Function(s: Scalar, _) => s.getCoverage
    }).toRight(LatisException("GroupByBinCount requires coverage metadata"))
      .flatMap { case (start, stop) =>
        val width = Math.abs(stop - start) / count.toDouble
        GroupByBinWidth(width, aggregation)
      }
  }

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = 

  def applyToModel(model: DataType): Either[LatisException, DataType] =
    //TODO: set binWidth
    //TODO: update cadence/resolution metadata
    model match {
      case f @ Function(domain: Scalar, _) => domain.valueType match {
        case _: NumericType =>
          aggregation.applyToModel(f).flatMap { range =>
            Function.from(domain, range)
          }
        case _ => LatisException("GroupByBinWidth expects a numeric domain variable").asLeft
      }
      case _ => LatisException("GroupByBinWidth expects a single domain variable").asLeft
    }
}

object GroupByBinWidth {

  def builder: OperationBuilder = (args: List[String]) => fromArgs(args)

  //TODO: support an aggregation arg
  def fromArgs(args: List[String]): Either[LatisException, GroupByBinWidth] = args match {
    case width :: Nil =>
      width.toDoubleOption.toRight(LatisException("Bin width must be numeric"))
        .flatMap(GroupByBinWidth(_))
    case _ => LatisException("GroupByBinWidth expects a single 'width' argument").asLeft
  }

  //TODO: don't use "apply" when returning Either
  def apply(
    width: Double,
    aggregation: Aggregation2 = DefaultAggregation2()
  ): Either[LatisException, GroupByBinWidth] = {
    Either.cond(width > 0, width, "Bin width must be a positive number")
      .map(new GroupByBinWidth(_, aggregation))
      .leftMap(LatisException(_))
  }
}
