package latis.units

/**
 * Defines the types of measurement.
 * These are used to categorize convertible units.
 */
sealed trait MeasurementType {
  def baseUnit: String
  //TODO: override equals
  //TODO: alias: K, ms, ...
  //TODO: derived Units: e.g. W/m^2
}

object Time extends MeasurementType {
  def baseUnit = "second"
}

object Length extends MeasurementType {
  def baseUnit = "meter"
}

object Temperature extends MeasurementType {
  def baseUnit = "kelvin"
}

object Irradiance extends MeasurementType {
  def baseUnit = "W/m^2"
}

object SpectralIrradiance extends MeasurementType {
  def baseUnit = "W/m^2/nm"
}
