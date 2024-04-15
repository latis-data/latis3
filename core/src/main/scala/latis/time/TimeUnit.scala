package latis.time

import cats.syntax.all.*

import latis.util.LatisException

/**
 * Defines a time duration unit in terms of a
 * multiplier of the base unit "seconds".
 */
case class TimeUnit(baseMultiplier: Double) {

  /** Represents a TimeUnit as a String */
  override def toString: String = baseMultiplier match {
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

  val Nanoseconds  = TimeUnit(1e-9)
  val Microseconds = TimeUnit(1e-6)
  val Milliseconds = TimeUnit(1e-3)
  val Seconds      = TimeUnit(1d)
  val Minutes      = TimeUnit(60d)
  val Hours        = TimeUnit(3600d)
  val Days         = TimeUnit(86400d)
  val Weeks        = TimeUnit(7d * 86400)
  val Years        = TimeUnit(365d * 86400)

  /**
   * Returns a TimeUnit given the unit's name.
   */
  def fromName(unit: String): Either[LatisException, TimeUnit] = unit match {
    case "nanoseconds"  => Nanoseconds.asRight
    case "microseconds" => Microseconds.asRight
    case "milliseconds" => Milliseconds.asRight
    case "seconds"      => Seconds.asRight
    case "minutes"      => Minutes.asRight
    case "hours"        => Hours.asRight
    case "days"         => Days.asRight
    case "weeks"        => Weeks.asRight
    case "years"        => Years.asRight
    case _ => LatisException(s"Invalid TimeUnit: $unit").asLeft
  }
}
