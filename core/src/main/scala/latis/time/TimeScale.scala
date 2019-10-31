package latis.time

import latis.units.{MeasurementScale, MeasurementType, Time => TimeType}

/**
 * Defines the MeasurementScale for time instances.
 * The given epoch serves as the zero of the time scale.
 * The timeUnit specifies the duration of each unit.
 */
class TimeScale(timeUnit: TimeUnit, epoch: String) extends MeasurementScale {
  def unitType: MeasurementType = TimeType
  override def baseMultiplier: Double = timeUnit.baseMultiplier
  override def zero: Double = - TimeFormat.parseIso(epoch)
}

object TimeScale {

  def apply(timeUnit: TimeUnit, epoch: String): TimeScale =
    new TimeScale(timeUnit, epoch)

  val Default = TimeScale(TimeUnit.Base, "1970-01-01")

  /**
   * Constructs a TimeScale from a "units" expression of the form:
   *   <time unit> since <epoch>
   *   or a TimeFormat expression.
   * A formatted time expression will translate to the default
   * TimeScale.
   */
  def apply(exp: String): TimeScale = exp.split(" since ") match {
    case Array(s, epoch) =>
      val unit = TimeUnit.fromName(s)
      TimeScale(unit, epoch)
    case Array(s) =>
      // Assumes a TimeFormat expression which
      // translates to the default numeric TimeScale
      TimeScale.Default
  }
}