package latis.ops

import java.time.Duration
import java.time.format.DateTimeParseException

import cats.effect.IO
import cats.syntax.all.*
import fs2.*

import latis.data.*
import latis.data.Data.DoubleValue
import latis.data.Data.StringValue
import latis.model.*
import latis.time.Time
import latis.util.LatisException

/**
 * Group Samples by binning the domain variable for a given width.
 *
 * This requires a Function with a one-dimensional, numeric or formatted 
 * time string domain. The [[width]] is assumed to be in the units of the 
 * domain variable. In the case of a string domain variable, the [[width]]
 * is assumed to be milliseconds. The resulting domain values will be the 
 * start of the bin, but bins with no samples will be dropped.
 * The optional [[aggregation]] operation will reduce the binned Samples
 * into a single Data object. By default, this will be a nested Function.
 */
class GroupByBinWidth private (
  width: Double,
  units: GroupByBinWidth.Units,
  aggregation: Aggregation2
) extends StreamOperation {
  //TODO: keep empty bins, preserve cadence and contiguity; use GroupByBin with DomainSet?
  //TODO: Index
  //TODO: support units
  //TODO: deal with duplicate id of inner and outer domain?
  //TODO: Should we require the aggregation to memoize? in the interest of serializing outer samples
  //TODO: inspect resulting chunk size for performance impact
  import GroupByBinWidth.Units.*

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = stream =>
    val aggF = aggregation.aggregateFunction(model)
    stream.groupAdjacentBy {
      case Sample(DomainData(Number(x)), _) =>
        units match {
          case Ms =>
            // ISO duration
            model match {
              case Function(t: Time, _) =>
                // converting the width to the proper units
                val multiplier = t.timeScale.baseMultiplier
                val parsedWidth = width / 1000 * (1/multiplier)
                Math.floor(x / parsedWidth) * parsedWidth
              case _ => throw LatisException("ISO duration width is incompatible with a numeric non-time domain")
            }
          case Domain =>
            // Round domain value down to start of bin
            Math.floor(x / width) * width
        }
      case Sample(DomainData(x: Text), _) =>
        // a string domain assumes width is in ms
        val time = model match {
          case Function(t: Time, _) =>
            t.valueAsDouble(x)
          case _ => throw LatisException("String domain assumes a Time scalar")
        }
        Math.floor(time / width) * width
      case _ => throw LatisException("Data sample does not match model")
    }.evalMap { case (value, chunk) =>
      aggF(Stream.chunk(chunk)).map { d =>
        model match {
          case Function(t: Time, _) =>
            t.timeFormat match {
              case Some(tf) =>
                // a string
                val time = tf.format(value.toLong)
                Sample(
                  DomainData(StringValue(time)),
                  RangeData(d)
                )
              case None =>
                // a number
                Sample(
                  DomainData(DoubleValue(value)),
                  RangeData(d)
                )
            }
          case Function(t: Scalar, _) =>
            Sample(
              DomainData(DoubleValue(value)),
              RangeData(d)
            )
          case _ => throw LatisException("Incorrect model for GroupByBinWidth")
        }
      }
    }

  /** Updating model's binWidth property */
  def applyToModel(model: DataType): Either[LatisException, DataType] = model match {
    case f @ Function(domain: Scalar, _) =>
      (domain, domain.valueType) match { 
        case (_: Time, StringValueType) | (_, _: NumericType)=>
          aggregation.applyToModel(f).flatMap { range =>
            val newDomain = updateDomainMetadata(domain)
            Function.from(newDomain, range)
          }
        case _ => LatisException("GroupByBinWidth expects a numeric domain or formatted string time variable").asLeft
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
        a  <- parseAggregation(agg)
        gb <- GroupByBinWidth.from(width, a)
      } yield gb
    case width :: Nil =>
      GroupByBinWidth.from(width)
    case _ =>
      val msg = "GroupByBinWidth expects a 'width' argument and an optional 'aggregation'"
      LatisException(msg).asLeft
  }

  private def parseAggregation(agg: String): Either[LatisException, Aggregation2] = agg match {
    case "count" => CountAggregation2().asRight
    case "stats" => StatsAggregation().asRight
    case _       => LatisException(s"Unsupported aggregation: $agg").asLeft
  }

  def from(
    width: String,
    aggregation: Aggregation2 = DefaultAggregation2()
  ): Either[LatisException, GroupByBinWidth] = {
    width.toDoubleOption match {
      case Some(w) =>
        Either.cond(w > 0, w, "Bin width must be a positive number")
          .map(GroupByBinWidth(_, Units.Domain, aggregation))
          .leftMap(LatisException(_))
      case None => 
        // an iso 8601 duration can't be negative
        if (width == "NaN") {
          Left(LatisException("Bin width must be a positive number"))
        } else {
          Either.catchOnly[DateTimeParseException]
            (Duration.parse(width).toMillis().toDouble)
            .map(GroupByBinWidth(_, Units.Ms, aggregation))
            .leftMap(LatisException(_))
        }
    }
  }

  enum Units:
    case Ms, Domain
}
