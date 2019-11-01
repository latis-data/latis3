package latis.units

import latis.time.TimeFormat

/**
 * A MeasurementScale defines how values of physical
 * quantities should be interpreted. This term was chosen over
 * the commonly used "unit of measure" to emphasize that
 * measurements require a zero in addition to the size of a unit.
 * This approach facilitates generalized affine unit conversions.
 */
trait MeasurementScale {
  def unitType: MeasurementType
  def baseMultiplier: Double = 1.0
  def zero: Double = 0
}

object Kelvin extends MeasurementScale {
  def unitType: MeasurementType = Temperature
}

object Celcius extends MeasurementScale {
  def unitType: MeasurementType = Temperature
  override def zero: Double = -273.15
}

object Farenheight extends MeasurementScale {
  def unitType: MeasurementType = Temperature
  override def baseMultiplier: Double = 5.0 / 9.0
  override def zero: Double = -459.67
}

object Meter extends MeasurementScale {
  def unitType: MeasurementType = Length
}

object KiloMeter extends MeasurementScale {
  def unitType: MeasurementType = Length
  override def baseMultiplier: Double = 1000
}
