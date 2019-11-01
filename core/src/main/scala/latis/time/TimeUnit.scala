package latis.time

/**
 * Defines a time duration unit in terms of a
 * multiplier of the base unit "seconds".
 */
case class TimeUnit(baseMultiplier: Double)

object TimeUnit {

  /**
   * Base time unit of seconds.
   */
  val Base = TimeUnit(1.0)

  /**
   * Returns a TimeUnit given the unit's name.
   */
  def fromName(unit: String): TimeUnit = unit match {
    case "nanoseconds"  => TimeUnit(1e-9)
    case "microseconds" => TimeUnit(1e-6)
    case "milliseconds" => TimeUnit(0.001)
    case "seconds"      => Base
    case "minutes"      => TimeUnit(60)
    case "hours"        => TimeUnit(3600)
    case "days"         => TimeUnit(86400)
    case "weeks"        => TimeUnit(7 * 86400)
    case "years"        => TimeUnit(365 * 86400)
  }
}
