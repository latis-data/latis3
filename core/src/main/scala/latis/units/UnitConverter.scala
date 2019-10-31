package latis.units

class UnitConverter(
    from: MeasurementScale,
    to: MeasurementScale) {
  //TODO: error if not the same UnitType

  private val scaleFactor: Double = from.baseMultiplier / to.baseMultiplier

  private val offset: Double = from.zero * scaleFactor - to.zero

  def convert(value: Double): Double = value * scaleFactor - offset
  //TODO: define as function, Id for same scales
}
