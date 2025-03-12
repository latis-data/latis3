package latis.ops

import cats.effect.IO
import cats.syntax.all.*
import fs2.*

import latis.data.*
import latis.data.Data.DoubleValue
import latis.model.*
import latis.util.LatisException

/**
 * Group Samples by binning the domain variable for a given width.
 *
 * This requires a Function with a one-dimensional, numeric domain.
 * The [[width]] is assumed to be in the units of the domain variable.
 * The optional [[aggregation]] operation will reduce the binned Samples
 * into a single Data object. By default, this will be a nested Function.
 */
class GroupByBinWidth private (
  width: Double,
  aggregation: Aggregation2 = DefaultAggregation2()
) extends StreamOperation {
  //TODO: time
  //TODO: Index
  //TODO: keep empty bins, preserve cadence and contiguity; use GroupByBin with DomainSet?
  //TODO: formatted time data,
  //TODO: width as ISO duration
  //TODO: support units
  //TODO: factor out groupBy function? Sample => DomainData, just extra baggage here
  //TODO: deal with duplicate id?
  //TODO: optional start value? The floor does create a reasonable start value
  //TODO: Should we expect the aggregation to memoize? in the interest on serializing outer samples
  //TODO: "gather"? streamable

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = stream =>
    val aggF = aggregation.aggregateFunction(model)
    stream.groupAdjacentBy {
      case Sample(DomainData(Number(n)), _) =>  Math.floor(n / width) * width
      case _ => throw LatisException("Data sample does not match model")
    }.evalMap { case (value, chunk) =>
      aggF(Stream.chunk(chunk)).map { d =>
        Sample(
          DomainData(DoubleValue(value)),
          RangeData(d)
        )
      }
    }

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
