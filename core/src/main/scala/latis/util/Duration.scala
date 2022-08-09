package latis.util

import java.time.temporal.ChronoUnit
import java.time.temporal.Temporal
import java.time.temporal.TemporalAmount
import java.time.temporal.TemporalUnit
import java.time.temporal.UnsupportedTemporalTypeException
import java.util.Objects

import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

/**
 * An implementation of a temporal duration compatible with ISO 8601
 * durations that plays nicely with the Java time API.
 */
final class Duration private (
  years: Long,
  months: Long,
  days: Long,
  hours: Long,
  minutes: Long,
  seconds: Long
) extends TemporalAmount {

  // The set of units supported by this TemporalAmount.
  //
  // Units must be listed in order from longest to shortest.
  private val units: List[TemporalUnit] = List(
    ChronoUnit.YEARS,
    ChronoUnit.MONTHS,
    ChronoUnit.DAYS,
    ChronoUnit.HOURS,
    ChronoUnit.MINUTES,
    ChronoUnit.SECONDS
  )

  override def addTo(temporal: Temporal): Temporal =
    temporal
      .plus(years, ChronoUnit.YEARS)
      .plus(months, ChronoUnit.MONTHS)
      .plus(days, ChronoUnit.DAYS)
      .plus(hours, ChronoUnit.HOURS)
      .plus(minutes, ChronoUnit.MINUTES)
      .plus(seconds, ChronoUnit.SECONDS)

  /**
   * Compares Durations for equality.
   *
   * The comparison is not done by the length of the duration but by
   * the number of each time unit the duration was constructed with.
   * This is because we don't know the actual length of the duration
   * without knowing when the duration is being applied. For instance,
   * whether `P31D` and `P1M` are equal depends on which month we're
   * talking about.
   */
  override def equals(x: Any): Boolean = x match {
    case d: Duration => units.forall(u => get(u) == d.get(u))
    case _ => false
  }

  override def get(unit: TemporalUnit): Long = unit match {
    case ChronoUnit.YEARS => years
    case ChronoUnit.MONTHS => months
    case ChronoUnit.DAYS => days
    case ChronoUnit.HOURS => hours
    case ChronoUnit.MINUTES => minutes
    case ChronoUnit.SECONDS => seconds
    case _ => throw new UnsupportedTemporalTypeException(unit.toString())
  }

  override def getUnits(): java.util.List[TemporalUnit] = units.asJava

  override def hashCode(): Int =
    Objects.hash(years, months, days, hours, minutes, seconds)

  override def subtractFrom(temporal: Temporal): Temporal =
    temporal
      .minus(years, ChronoUnit.YEARS)
      .minus(months, ChronoUnit.MONTHS)
      .minus(days, ChronoUnit.DAYS)
      .minus(hours, ChronoUnit.HOURS)
      .minus(minutes, ChronoUnit.MINUTES)
      .minus(seconds, ChronoUnit.SECONDS)
}

object Duration {

  // Inspired by https://rgxdb.com/r/MD2234J
  //
  // fromIsoString assumes only valid longs will be matched in the
  // capture groups
  private val regex: Regex =
    """^P(?=\d|T\d)(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$""".r

  def fromIsoString(str: String): Either[LatisException, Duration] = {
    // This assumes the regex will only match valid longs in the
    // capture groups.
    def longOrZero(m: Regex.Match, i: Int): Long =
      Option(m.group(i)).map(_.toLong).getOrElse(0L)

    regex.findFirstMatchIn(str).map { m =>
      Duration.of(
        longOrZero(m, 1),
        longOrZero(m, 2),
        longOrZero(m, 3),
        longOrZero(m, 4),
        longOrZero(m, 5),
        longOrZero(m, 6)
      )
    }.toRight(LatisException(s"Failed to parse duration: $str"))
  }

  // This is private because changing methods with default arguments
  // will lead to binary compatibility issues. I could see maybe
  // adding milliseconds in the future.
  private[util] def of(
    years: Long = 0,
    months: Long = 0,
    days: Long = 0,
    hours: Long = 0,
    minutes: Long = 0,
    seconds: Long = 0
  ): Duration = new Duration(years, months, days, hours, minutes, seconds)
}
