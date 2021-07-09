package latis.units

//TODO: See squants Dimension: https://www.javadoc.io/doc/org.typelevel/squants_2.13/1.6.0/squants/Dimension.html

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
