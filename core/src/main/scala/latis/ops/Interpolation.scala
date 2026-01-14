package latis.ops

import cats.syntax.all.*

import latis.data.*
import latis.model.*
import latis.ops.Interpolation.*
import latis.util.*

/**
 * Trait for interpolation algorithms.
 *
 * Given a list of Samples, returns a single Sample based on
 * the given domain value.
 *
 * This requires a 1D domain and expects the value type to
 * be consistent with the domain Scalar.
 */
sealed trait Interpolation {

  def interpolate(
    model: DataType,
    samples: List[Sample],
    value: Datum
  ): Either[LatisException, Sample]

}

object Interpolation {

  /**
   * Constructs an Interpolation by its (case-insensitive) name.
   *
   * Supported interpolation strategies:
   *  - `floor`: Returns the "lowest" of the bounding Samples
   *  - `linear`: Computes a linear interpolation between the bounding Samples
   *  - `near/nearest`: Returns the nearest of the bounding Samples
   *  - `none`: No interpolation will be done, Sample must match
   */
  def fromName(name: String): Either[LatisException, Interpolation] =
    name.toLowerCase match {
      case "floor" => FloorInterpolation().asRight
      case "linear" => LinearInterpolation().asRight
      case "near" | "nearest" => NearestInterpolation().asRight
      case "none" => NoInterpolation().asRight
      case s => LatisException(s"Unsupported interpolation type: $s").asLeft
    }

  //---- Utility Methods ----//

  /** Extracts the domain Scalar assuming a 1D, non-Index Function. */
  def getDomainVar(model: DataType): Either[LatisException, Scalar] =
    model match {
      case Function(scalar: Scalar, _) if (! scalar.isInstanceOf[Index]) =>
        scalar.asRight
      case _ =>
        LatisException("Interpolation expects a 1D non-index domain").asLeft
    }

  /** Extracts the domain data from an assumed arity-1 Sample as a Datum */
  def getDomainData(sample: Sample): Either[LatisException, Datum] =
    sample match {
      case Sample((d: Datum) :: Nil, _) => d.asRight
      case _ =>
        LatisException("Interpolation expects a 1D non-index domain").asLeft
    }
}

//==== No Interpolation (equality only) ====//

/** Returns a Sample only if its domain value is equivalent */
case class NoInterpolation() extends Interpolation {

  override def interpolate(
    model: DataType,
    samples: List[Sample],
    value: Datum
  ): Either[LatisException, Sample] = {
    for {
      domainVar <- getDomainVar(model)
      ordering   = domainVar.ordering
      before    <- samples.filterA { sample =>
        getDomainData(sample).map(ordering.lteq(_, value))
      }
      last      <- before.lastOption.toRight {
        LatisException("Interpolation cannot extrapolate")
      }
      sample    <- getDomainData(last).flatMap { n =>
        if (ordering.equiv(n, value)) last.asRight
        else LatisException("NoInterpolation will not interpolate").asLeft
      }
    } yield sample
  }
}

//==== Floor (round down) Interpolation ====//

/** Returns the Sample that would immediately precede or equal the given domain value */
case class FloorInterpolation() extends Interpolation {
  //TODO: error if no greater sample? essentially extrapolation

  override def interpolate(
    model: DataType,
    samples: List[Sample],
    value: Datum
  ): Either[LatisException, Sample] = {
    for {
      domainVar <- getDomainVar(model)
      ordering   = domainVar.ordering
      before    <- samples.filterA { sample =>
        getDomainData(sample).map(ordering.lteq(_, value))
      }
      last      <- before.lastOption.toRight {
        LatisException("Interpolation cannot extrapolate")
      }
    } yield Sample(DomainData(value), last.range)
  }
}

//==== Nearest Neighbor Interpolation ====//

/**
 * Returns the Sample closest to the given domain value,
 * rounding up if equidistant.
 */
case class NearestInterpolation() extends Interpolation {

  override def interpolate(
    model: DataType,
    samples: List[Sample],
    value: Datum
  ): Either[LatisException, Sample] = {
    for {
      number <- value match {
        case num: Number => num.asRight
        case _ =>
          val msg = "Nearest neighbor interpolation requires a numeric value"
          LatisException(msg).asLeft
      }
      domainVar <- getDomainVar(model)
      ordering = domainVar.ordering
      before <- samples.filterA { sample =>
        getDomainData(sample).map(ordering.lteq(_, number))
      }
      after <- samples.filterA { sample =>
        getDomainData(sample).map(ordering.gt(_, number))
      }
      s1 <- before.lastOption
        .toRight(LatisException("Interpolation cannot extrapolate"))
      s2 <- after.headOption match {
        case Some(s) => s.asRight
        case None => getDomainData(s1).flatMap { d =>
          // If no samples greater than, see if number matches last sample
          if (ordering.equiv(d, number)) s1.asRight
          else LatisException("Interpolation cannot extrapolate").asLeft
        }
      }
      d1 <- getDomainData(s1).flatMap {
        case num: Number => num.asDouble.asRight
        case _ =>
          val msg = "Nearest neighbor interpolation requires numeric data"
          LatisException(msg).asLeft
      }
      d2 <- getDomainData(s2).flatMap {
        case num: Number => num.asDouble.asRight
        case _ =>
          val msg = "Nearest neighbor interpolation requires numeric data"
          LatisException(msg).asLeft
      }
    } yield {
      val dd1 = math.abs(d1 - number.asDouble)
      val dd2 = math.abs(d2 - number.asDouble)
      // Note: rounds up if eq, consistent with math.round
      if (dd1 < dd2) Sample(DomainData(number), s1.range)
      else Sample(DomainData(number), s2.range)
    }
  }
}

//==== Linear Interpolation ====//

/**
 * Interpolates the given samples for the given domain value using
 * linear interpolation between bounding samples.
 *
 * All range variables must be of type Real (with floating point
 * values) since a new value will be computed as a double and can't
 * safely be converted to a non-real data type. The domain data
 * can be any numeric variable since it is only used in the
 * computation.
 *
 * TODO: use fill values for non-Real data?
 */
case class LinearInterpolation() extends Interpolation {

  override def interpolate(
    model: DataType,
    samples: List[Sample],
    value: Datum
  ): Either[LatisException, Sample] = {
    for {
      number <- value match {
        case num: Number => num.asRight
        case _ =>
          val msg = "Linear interpolation requires a numeric domain"
          LatisException(msg).asLeft
      }
      domainVar <- getDomainVar(model)
      ordering   = domainVar.ordering
      before    <- samples.filterA { sample =>
        getDomainData(sample).map(ordering.lteq(_, number))
      }
      after <- samples.filterA { sample =>
        getDomainData(sample).map(ordering.gt(_, number))
      }
      s1 <- before.lastOption.toRight(LatisException("Interpolation cannot extrapolate"))
      s2 <- after.headOption match {
        case Some(s) => s.asRight
        case None    => getDomainData(s1).flatMap { d =>
          // If no samples greater than, see if number matches last sample
          if (ordering.equiv(d, number)) s1.asRight
          else LatisException("Interpolation cannot extrapolate").asLeft
        }
      }
      d1 <- getDomainData(s1).flatMap {
        case num: Number => num.asDouble.asRight
        case _ =>
          val msg = "Linear interpolation requires numeric domain"
          LatisException(msg).asLeft
      }
      d2 <- getDomainData(s2).flatMap {
        case num: Number => num.asDouble.asRight
        case _ =>
          val msg = "Linear interpolation requires numeric domain"
          LatisException(msg).asLeft
      }
      sample <-
        // Don't interp if number equals domain value
        if (d1 == number.asDouble) s1.asRight
        else if (d2 == number.asDouble) s2.asRight
        else interp(model, s1, s2, number)
    } yield sample
  }

  private def interp(model: DataType, s1: Sample, s2: Sample, num: Number): Either[LatisException, Sample] = {
    for {
      x1 <- getDomainData(s1).flatMap(getDoubleVal)
      x2 <- getDomainData(s2).flatMap(getDoubleVal)
      ds <- (model.getScalars.tail, s1.range, s2.range).traverseN {
        case (s: Scalar, Real(d1), Real(d2)) =>
          val d = (d2 - d1) * (num.asDouble - x1) / (x2 - x1) + d1
          s.valueType.convertDouble(d).get.asRight
        case _ =>
          val msg = "Linear interpolation required floating point data"
          LatisException(msg).asLeft
      }
    } yield {
      Sample(DomainData(num), RangeData(ds))
    }
  }

  private def getDoubleVal(data: Datum): Either[LatisException, Double] = {
    data match {
      case Number(d) => d.asRight
      case _ =>
        val msg = "Numeric data expected"
        LatisException(msg).asLeft
    }
  }
}
