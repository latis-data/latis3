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
 * The resulting domain values will be the start of the bin, but bins
 * with no samples will be dropped.
 * The optional [[aggregation]] operation will reduce the binned Samples
 * into a single Data object. By default, this will be a nested Function.
 */
class GroupByBinWidth private (
  width: Double,
  aggregation: Aggregation2
) extends StreamOperation {
  //TODO: keep empty bins, preserve cadence and contiguity; use GroupByBin with DomainSet?
  //TODO: formatted time data
  //TODO: width as ISO duration
  //TODO: Index
  //TODO: support units
  //TODO: deal with duplicate id of inner and outer domain?
  //TODO: Should we require the aggregation to memoize? in the interest of serializing outer samples
  //TODO: inspect resulting chunk size for performance impact

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = stream =>
    val aggF = aggregation.aggregateFunction(model)
    stream.groupAdjacentBy {
      case Sample(DomainData(Number(x)), _) =>
        // Round domain value down to start of bin
        Math.floor(x / width) * width
      case _ => throw LatisException("Data sample does not match model")
    }.evalMap { case (value, chunk) =>
      aggF(Stream.chunk(chunk)).map { d =>
        Sample(
          DomainData(DoubleValue(value)),
          RangeData(d)
        )
      }
    }

  def applyToModel(model: DataType): Either[LatisException, DataType] = model match {
    case f @ Function(domain: Scalar, _) => domain.valueType match {
      case _: NumericType =>
        aggregation.applyToModel(f).flatMap { range =>
          val newDomain = updateDomainMetadata(domain)
          Function.from(newDomain, range)
        }
      case _ => LatisException("GroupByBinWidth expects a numeric domain variable").asLeft
    }
    case _ => LatisException("GroupByBinWidth expects a single domain variable").asLeft
  }

  /** Update the metadata of the domain Scalar */
  private def updateDomainMetadata(s: Scalar): Scalar = {
    //TODO: update cadence/resolution metadata
    val meta = s.metadata + ("binWidth" -> width.toString)
    Scalar.fromMetadata(meta).fold(throw _, identity) //should be safe
  }

  override def toString = s"GroupByBinWidth($width) with $aggregation"
}

object GroupByBinWidth {

  def builder: OperationBuilder = (args: List[String]) => args match {
    case width :: agg :: Nil =>
      for {
        w  <- parseWidth(width)
        a  <- parseAggregation(agg)
        gb <- GroupByBinWidth.from(w, a)
      } yield gb
    case width :: Nil =>
      parseWidth(width).flatMap(GroupByBinWidth.from(_))
    case _ =>
      val msg = "GroupByBinWidth expects a 'width' argument and an optional 'aggregation'"
      LatisException(msg).asLeft
  }

  private def parseWidth(width: String): Either[LatisException, Double] =
    width.toDoubleOption.toRight(LatisException("Bin width must be numeric"))

  private def parseAggregation(agg: String): Either[LatisException, Aggregation2] = agg match {
    case "count" => CountAggregation2().asRight
    case "stats" => StatsAggregation().asRight
    case _       => LatisException(s"Unsupported aggregation: $agg").asLeft
  }

  def from(
    width: Double,
    aggregation: Aggregation2 = DefaultAggregation2()
  ): Either[LatisException, GroupByBinWidth] = {
    Either.cond(width > 0, width, "Bin width must be a positive number")
      .map(GroupByBinWidth(_, aggregation))
      .leftMap(LatisException(_))
  }
}
