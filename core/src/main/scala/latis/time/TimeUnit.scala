package latis.time

import cats.syntax.all._

import latis.util.LatisException

/**
 * Defines a time duration unit in terms of a
 * multiplier of the base unit "seconds".
 */
case class TimeUnit(baseMultiplier: Double) {

  /** Represents a TimeUnit as a String */
  override def toString() = baseMultiplier match {
    case 1e-9     => "nanoseconds"
    case 1e-6     => "microseconds"
    case 0.001    => "milliseconds"
    case 1        => "seconds"
    case 60       => "minutes"
    case 3600     => "hours"
    case 86400    => "days"
    case 604800   => "weeks"
    case 31536000 => "years"
    case d        => s"$d-seconds"
  }
}

object TimeUnit {

  /**
   * Base time unit of seconds.
   */
  val Base = TimeUnit(1.0)

  /**
   * Returns a TimeUnit given the unit's name.
   */
  def fromName(unit: String): Either[LatisException, TimeUnit] = unit match {
    case "nanoseconds"  => TimeUnit(1e-9).asRight
    case "microseconds" => TimeUnit(1e-6).asRight
    case "milliseconds" => TimeUnit(0.001).asRight
    case "seconds"      => Base.asRight
    case "minutes"      => TimeUnit(60).asRight
    case "hours"        => TimeUnit(3600).asRight
    case "days"         => TimeUnit(86400).asRight
    case "weeks"        => TimeUnit(7 * 86400).asRight
    case "years"        => TimeUnit(365 * 86400).asRight
    case _ => LatisException(s"Invalid TimeUnit: $unit").asLeft
  }

  //TODO: make enumeration of units for safer general usage
}
