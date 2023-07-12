package latis.time

import java.time.*
import java.time.format.DateTimeFormatter
import java.time.temporal.*
import java.util.Locale

import scala.util.*

import cats.syntax.all.*

import latis.util.LatisException

/**
 * Time parsing and formatting support.
 *
 * This wraps a thread-safe DateTimeFormatter. The formatting patterns are defined at
 * https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html.
 */
class TimeFormat private (format: String, dtf: DateTimeFormatter) {
  //TODO: support 2-digit century start (default assumes 2000-2999)

  /** Formats the time given in epoch milliseconds. */
  def format(millis: Long): String =
    dtf.format(Instant.ofEpochMilli(millis))

  /** Parses the given time string as epoch milliseconds. */
  def parse(string: String): Either[LatisException, Long] =
    (for {
      ta <- Either.catchNonFatal(dtf.parse(string))
      ms <- Either.catchNonFatal(ta.query(TimeFormat.temporalQuery))
    } yield ms).leftMap {
      val msg = s"Could not parse '$string' with the format $format"
      LatisException(msg, _)
    }

  /**
   * Returns the format string.
   */
  override def toString: String = format
}


object TimeFormat {

  /**
   * Makes a TimeFormat for the given pattern as supported by
   * java.time.format.DateTimeFormatter.
   *
   * This imposes the use of the US locale and GMT time zone.
   */
  def fromExpression(format: String): Either[LatisException, TimeFormat] =
    Either.catchNonFatal {
      DateTimeFormatter.ofPattern(format)
        .withLocale(Locale.US)
        .withZone(ZoneId.of("GMT"))
    }.map { dtf =>
      new TimeFormat(format, dtf)
    }.leftMap {
      val msg = s"Could not parse '$format' as a time format."
      LatisException(msg, _)
    }

  /** Returns whether the given pattern is a valid time format. */
  def isValid(format: String): Boolean = fromExpression(format).isRight

  /**
   * Provides a TimeFormat for the default ISO 8601 format:
   * yyyy-MM-dd'T'HH:mm:ss.SSS'Z'.
   */
  val Iso: TimeFormat = TimeFormat.fromExpression("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    .fold(throw _, identity) //Should be safe

  /**
   * Formats the given time in epoch milliseconds in the
   * default ISO 8601 time format: yyyy-MM-dd'T'HH:mm:ss.SSS'Z'.
   */
  def formatIso(millis: Long): String = Iso.format(millis)

  /**
   * Parses an arbitrary ISO 8601 time string as epoch milliseconds.
   *
   * If this needs to be applied numerous times,
   * e.g. for each sample of a dataset, it is recommended that a
   * TimeFormat object be created with fromIsoValue and reused.
   */
  def parseIso(time: String): Either[LatisException, Long] =
    for {
      formatter <- fromIsoValue(time)
      tvalue    <- formatter.parse(time)
    } yield tvalue

  /**
   * Creates a TimeFormat from an ISO time value.
   *
   * This supports the following ISO 8601 representations:
   *
   *   Date:
   *   - yyyy
   *   - yyyy-MM
   *   - yyyyDDD
   *   - yyyy-DDD
   *   - yyyyMMdd
   *   - yyyy-MM-dd
   *
   *   Time of day:
   *   - HH
   *   - HHmm
   *   - HH:mm
   *   - HHmmss
   *   - HHmmss.S
   *   - HHmmss.SS
   *   - HHmmss.SSS
   *   - HHmmss.SSSS
   *   - HHmmss.SSSSS
   *   - HHmmss.SSSSSS
   *   - HHmmss.SSSSSSS
   *   - HHmmss.SSSSSSSS
   *   - HHmmss.SSSSSSSSS
   *   - HH:mm:ss
   *   - HH:mm:ss.S
   *   - HH:mm:ss.SS
   *   - HH:mm:ss.SSS
   *   - HH:mm:ss.SSSS
   *   - HH:mm:ss.SSSSS
   *   - HH:mm:ss.SSSSSS
   *   - HH:mm:ss.SSSSSSS
   *   - HH:mm:ss.SSSSSSSS
   *   - HH:mm:ss.SSSSSSSSS
   *
   *   with an optional "Z" time zone designator.
   *
   *   The data and time of day components must be delimited by a "T".
   */
  def fromIsoValue(value: String): Either[LatisException, TimeFormat] = {
    (value.split("T") match { //TODO: or space? allowed in profile RFC 3339
      case Array(date) =>
        // No time component
        getDateFormatString(date)
      case Array(date, time) =>
        for {
          d <- getDateFormatString(date)
          t <- getTimeFormatString(time)
        } yield List(d, t).mkString("'T'")
      case _ =>
        LatisException(s"Invalid TimeFormat: $value").asLeft
    }).flatMap {
      TimeFormat.fromExpression
    }
  }

  /**
   * Converts the date portion of an ISO time to a format pattern.
   */
  private def getDateFormatString(s: String): Either[LatisException, String] = {
    if      (s.matches(raw"\d{4}-\d{2}-\d{2}")) "yyyy-MM-dd".asRight
    else if (s.matches(raw"\d{8}"))             "yyyyMMdd".asRight
    else if (s.matches(raw"\d{7}"))             "yyyyDDD".asRight
    else if (s.matches(raw"\d{4}-\d{3}"))       "yyyy-DDD".asRight
    else if (s.matches(raw"\d{4}-\d{2}"))       "yyyy-MM".asRight
    else if (s.matches(raw"\d{4}"))             "yyyy".asRight
    else {
      val msg = s"Failed to determine a date format for $s"
      LatisException(msg).asLeft
    }
  }

  /**
   * Converts the time portion of an ISO time to a format pattern.
   */
  private def getTimeFormatString(s: String): Either[LatisException, String] = {
    // Strip off the optional "Z" time zone designation to add back later
    val (s2, z) = if (s.endsWith("Z")) (s.dropRight(1), "'Z'") else (s, "")

    (if (s2.matches(raw"\d{2}:\d{2}:\d{2}\.\d{9}"))     "HH:mm:ss.SSSSSSSSS".asRight
    else if (s2.matches(raw"\d{2}:\d{2}:\d{2}\.\d{8}")) "HH:mm:ss.SSSSSSSS".asRight
    else if (s2.matches(raw"\d{2}:\d{2}:\d{2}\.\d{7}")) "HH:mm:ss.SSSSSSS".asRight
    else if (s2.matches(raw"\d{2}:\d{2}:\d{2}\.\d{6}")) "HH:mm:ss.SSSSSS".asRight
    else if (s2.matches(raw"\d{2}:\d{2}:\d{2}\.\d{5}")) "HH:mm:ss.SSSSS".asRight
    else if (s2.matches(raw"\d{2}:\d{2}:\d{2}\.\d{4}")) "HH:mm:ss.SSSS".asRight
    else if (s2.matches(raw"\d{2}:\d{2}:\d{2}\.\d{3}")) "HH:mm:ss.SSS".asRight
    else if (s2.matches(raw"\d{2}:\d{2}:\d{2}\.\d{2}")) "HH:mm:ss.SS".asRight
    else if (s2.matches(raw"\d{2}:\d{2}:\d{2}\.\d{1}")) "HH:mm:ss.S".asRight
    else if (s2.matches(raw"\d{2}:\d{2}:\d{2}"))        "HH:mm:ss".asRight
    else if (s2.matches(raw"\d{6}\.\d{9}"))             "HHmmss.SSSSSSSSS".asRight
    else if (s2.matches(raw"\d{6}\.\d{8}"))             "HHmmss.SSSSSSSS".asRight
    else if (s2.matches(raw"\d{6}\.\d{7}"))             "HHmmss.SSSSSSS".asRight
    else if (s2.matches(raw"\d{6}\.\d{6}"))             "HHmmss.SSSSSS".asRight
    else if (s2.matches(raw"\d{6}\.\d{5}"))             "HHmmss.SSSSS".asRight
    else if (s2.matches(raw"\d{6}\.\d{4}"))             "HHmmss.SSSS".asRight
    else if (s2.matches(raw"\d{6}\.\d{3}"))             "HHmmss.SSS".asRight
    else if (s2.matches(raw"\d{6}\.\d{2}"))             "HHmmss.SS".asRight
    else if (s2.matches(raw"\d{6}\.\d{1}"))             "HHmmss.S".asRight
    else if (s2.matches(raw"\d{6}"))                    "HHmmss".asRight
    else if (s2.matches(raw"\d{2}:\d{2}"))              "HH:mm".asRight
    else if (s2.matches(raw"\d{4}"))                    "HHmm".asRight
    else if (s2.matches(raw"\d{2}"))                    "HH".asRight
    else {
      val msg = s"Failed to determine a time format for $s"
      LatisException(msg).asLeft
    }).map(_ + z) //add optional Z back on
  }

  /**
   * Defines a TemporalQuery for extracting epoch milliseconds from a
   * TemporalAccessor resulting from a DateTimeFormatter parse.
   */
  private val temporalQuery = new TemporalQuery[Long] {
    def queryFrom(ta: TemporalAccessor): Long = {
      val milliOfDay = if (ta.isSupported(ChronoField.MILLI_OF_DAY)) {
        ta.getLong(ChronoField.MILLI_OF_DAY)
      } else 0L

      val days = if (ta.isSupported(ChronoField.EPOCH_DAY)) {
        ta.getLong(ChronoField.EPOCH_DAY)
      } else if (ta.isSupported(ChronoField.MONTH_OF_YEAR)) {
        val year  = ta.get(ChronoField.YEAR)
        val month = ta.get(ChronoField.MONTH_OF_YEAR)
        LocalDate.of(year, month, 1).getLong(ChronoField.EPOCH_DAY)
      } else if (ta.isSupported(ChronoField.YEAR)) {
        val year = ta.get(ChronoField.YEAR)
        LocalDate.of(year, 1, 1).getLong(ChronoField.EPOCH_DAY)
      } else throw new DateTimeException("Failed to query TemporalAccessor")

      days * 86400000L + milliOfDay
    }
  }
}
