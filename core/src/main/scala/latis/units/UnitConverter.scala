package latis.units

import cats.syntax.all._

import latis.time.TimeConverter
import latis.time.TimeScale
import latis.util.LatisException

class UnitConverter protected (from: MeasurementScale, to: MeasurementScale) {

  private val scaleFactor: Double = from.baseMultiplier / to.baseMultiplier

  private val offset: Double = from.zero * scaleFactor - to.zero

  def convert(value: Double): Double = value * scaleFactor - offset
}

object UnitConverter {

  /** Constructs a UnitConverter from two MeasurementScales. */
  def fromScales(
    from: MeasurementScale,
    to: MeasurementScale
  ): Either[LatisException, UnitConverter] = (from, to) match {
    case _ if (from.unitType != to.unitType) => LatisException("Incompatible measurement types").asLeft
    case (ts1: TimeScale, ts2: TimeScale) => TimeConverter(ts1, ts2).asRight
    case _ => new UnitConverter(from, to).asRight
  }
}
