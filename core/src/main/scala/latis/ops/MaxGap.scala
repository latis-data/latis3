package latis.ops

import cats.effect.IO
import cats.syntax.all.*
import fs2.*

import latis.data.*
import latis.model.*
import latis.util.LatisException

/**
 * Inserts Samples such that the domain values have no gap greater
 * than the given [[gapSize]], using the given Interpolation algorithm.
 * The resulting cadence will be the gap size.
 *
 * Requires an arity-1 Function with a numeric domain variable, and
 * assumes the gap size is in the numeric units of that variable.
 * Text Time variables will expect epoch milliseconds. ISO 8601 durations
 * for time variables are not yet supported.
 */
class MaxGap private (gapSize: Double, interp: Interpolation) extends StreamOperation {

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = stream => {

    val domainVar: Scalar = model match {
      case Function(domain: Scalar, _)
        if (domain.valueType.isInstanceOf[NumericType]) => domain
      case _ => throw LatisException("MaxGap requires a single numeric domain variable")
    }

    // Define multiplier to support descending order
    val order = if (domainVar.ascending) 1.0 else -1.0

    // Loop, dropping samples that have the same target variable value.
    def go(prev: Sample, rest: Stream[IO, Sample]): Pull[IO, Sample, Unit] = {
      rest.pull.uncons1.flatMap {
        case Some((curr, tail)) =>
          //Note that an invalid sample will result in a NaN
          //  which will fail the gap test thus no sample added.
          val d0 = domainVar.valueAsDouble(getDomainData(prev))
          val d1 = domainVar.valueAsDouble(getDomainData(curr))
          if (math.abs(d1 - d0) > gapSize) {
            val d = domainVar.valueType.convertDouble(d0 + order * gapSize).map {
              case num: Number => num
              case _ => throw LatisException("MaxGap data must be numeric")
            }.getOrElse(throw LatisException("MaxGap unable to make new sample"))
            val insert = interp.interpolate(model, List(prev, curr), d)
              .fold(throw _, identity) //invalid sample or bug
            Pull.output1(insert) >> go(insert, Stream.emit(curr) ++ tail)
          } else Pull.output1(curr) >> go(curr, tail)
        case None =>
          Pull.done
      }
    }

    // Get and emit first sample then start iterating
    stream.pull.uncons1.flatMap {
      case Some((first, tail)) =>
        Pull.output1(first) >> go(first, tail)
      case None =>
        Pull.done
    }.stream
  }

  // Get the Data object of the domain variable from a Sample
  private def getDomainData(sample: Sample): Datum = sample match {
    case Sample(d :: Nil, _) => d
    case _ => throw LatisException("MaxGap expects a 1D domain")
  }

  // Model is unchanged
  override def applyToModel(model: DataType): Either[LatisException, DataType] =
    model.asRight
}

object MaxGap {

  def builder: OperationBuilder = (args: List[String]) => fromArgs(args)

  def fromArgs(args: List[String]): Either[LatisException, MaxGap] = args match {
    case s1 :: s2 :: Nil =>
      for {
        value  <- parseGapSize(s1)
        interp <- parseInterpolation(s2)
      } yield MaxGap(value, interp)
    case _ => LatisException("MaxGap requires a gap size and interpolation").asLeft
  }

  private def parseGapSize(s: String): Either[LatisException, Double] = {
    //TODO: support ISO 8601 duration, need model to get units right
    s.toDoubleOption.filter(_ > 0)
      .toRight(LatisException("Gap size must be a positive number"))
  }

  private def parseInterpolation(s: String): Either[LatisException, Interpolation] =
    Interpolation.fromName(s).flatMap {
      case NoInterpolation() => LatisException("MaxGap requires interpolation").asLeft
      case interp => interp.asRight
    }
}
