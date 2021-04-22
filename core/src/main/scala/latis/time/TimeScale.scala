package latis.time

import java.util.Date
import java.util.GregorianCalendar
import java.util.TimeZone

import cats.syntax.all._

import latis.units.{Time => TimeType}
import latis.units.MeasurementScale
import latis.units.MeasurementType
import latis.util.LatisException

/**
 * Defines the MeasurementScale for time instances.
 * The given epoch serves as the zero of the time scale.
 * The timeUnit specifies the duration of each unit.
 *
 * Since there is no absolute zero for time scales, this uses the
 * Java time scale (milliseconds since 1970-01-01T00:00:00) as the default.
 * The zero of a time scale is the offset of its epoch from the default epoch
 * in its units. For example, "hours since 1970-01-02" would be at the standard
 * time scale's zero (1970-01-01) one day before it's own zero (i.e. epoch). In
 * it's units (hours) the zero (one day ago) would be -24.
 */
case class TimeScale(timeUnit: TimeUnit, epoch: Date) extends MeasurementScale {

  def unitType: MeasurementType = TimeType

  override def baseMultiplier: Double = timeUnit.baseMultiplier

  override def zero: Double = -epoch.getTime / 1000 / baseMultiplier

  override def toString() = s"$timeUnit since ${TimeFormat.formatIso(epoch.getTime)}"
}

object TimeScale {

  /**
   * Defines the default time scale as Java's default:
   * milliseconds since 1970.
   */
  val Default: TimeScale = TimeScale(TimeUnit(0.001), new Date(0))

  /**
   * Defines a TimeScale for Julian Date: days since noon UT on Jan 1, 4713 BC.
   *
   * Although the epoch is "Julian", this special case is here solely to
   * represent the unique epoch. This simply represents the number of 24-hour
   * days since this epoch in a manner consistent with the proleptic
   * Gregorian calendar which is appropriate for modern scientific data.
   *
   * Note that this overrides the usual "units since epoch" form of toString
   * with "Julian Date".
   */
  lazy val Julian = {
    // Java's default calendar jumps from 1 BC to 1 AD, we need to use year -4712
    val cal = new GregorianCalendar(-4712, 0, 1, 12, 0)
    cal.setTimeZone(TimeZone.getTimeZone("GMT"))
    new TimeScale(TimeUnit(86400), cal.getTime) {
      override def toString() = "Julian Date"
    }
  }

  /**
   * Constructs a TimeScale from a "units" expression of the form:
   *   <time unit> since <epoch>
   *   or "Julian Date" (and similar forms starting with "julian")
   *   or a TimeFormat expression.
   * A formatted time expression will be handled with the default
   * numeric TimeScale.
   */
  def fromExpression(units: String): Either[LatisException, TimeScale] =
    units.split("""\s+since\s+""") match {
      case Array(u, e) =>
        for {
          unit <- TimeUnit.fromName(u)
          epoch <- TimeFormat.parseIso(e)
          date = new Date(epoch)
        } yield TimeScale(unit, date)
      case Array(s) =>
        // Check if some form of "Julian Date"
        if (s.toLowerCase.startsWith("julian")) TimeScale.Julian.asRight
        // Otherwise assumes a TimeFormat expression which
        // is handled with the default numeric TimeScale
        //TODO: fail if not a valid time format?
        else TimeScale.Default.asRight
      case _ =>
        //Only if "since" appears more than once?
        LatisException(s"Invalid TimeScale expression: $units").asLeft
    }
}
