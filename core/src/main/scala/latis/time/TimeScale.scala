package latis.time

import latis.units.{Time => TimeType}
import latis.units.MeasurementScale
import latis.units.MeasurementType

/**
 * Defines the MeasurementScale for time instances.
 * The given epoch serves as the zero of the time scale.
 * The timeUnit specifies the duration of each unit.
 *
 * Since there is no absolute zero for time scales, this uses the
 * Java time scale (milliseconds since 1970-01-01T00:00:00) as the standard.
 * The zero of a time scale is the offset of its epoch from the default epoch
 * in its units. For example, "hours since 1970-01-02" would be at the standard
 * time scale's zero (1970-01-01) one day before it's own zero (i.e. epoch). In
 * it's units (hours) the zero (one day ago) would be -24.
 */
case class TimeScale(timeUnit: TimeUnit, epoch: String) extends MeasurementScale {
  def unitType: MeasurementType = TimeType

  override def baseMultiplier: Double = timeUnit.baseMultiplier

  override def zero: Double =
    -TimeFormat
      .parseIso(epoch)
      .map(_ / 1000 / baseMultiplier)
      .getOrElse { ??? }

  override def toString() = s"$timeUnit since $epoch"
}

object TimeScale {
  /**
   * Defines the default time scale as Java's default:
   * milliseconds since 1970.
   */
  val Default: TimeScale = TimeScale(TimeUnit(0.001), "1970-01-01")

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
    case Array(_) =>
      // Assumes a TimeFormat expression which
      // translates to the default numeric TimeScale
      TimeScale.Default
  }
}
