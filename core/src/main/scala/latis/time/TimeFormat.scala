package latis.time

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Date, Locale, TimeZone}

/**
 * TimeFormat support that is thread safe and assumes GMT time zone.
 */
class TimeFormat(format: String) {

  private val sdf: SimpleDateFormat = {
    val sdf = try new SimpleDateFormat(format, Locale.ENGLISH) catch {
      case e: Exception => throw new IllegalArgumentException(
        s"Could not parse '$format' as a time format: ${e.getMessage}")
    }
    sdf.setTimeZone(TimeZone.getTimeZone("GMT"))
    sdf
  }

  def format(millis: Long): String = this.synchronized {
    sdf.format(new Date(millis))
  }

  def parse(string: String): Long = this.synchronized {
    try {
      sdf.parse(string).getTime
    } catch {
      case e: ParseException =>
        val msg = s"Unable to parse time string ($string) with the format $format"
        throw new IllegalArgumentException(msg)
    }
  }

  /**
   * Sets the 100-year period to be used to interpret 2-digit years.
   * This time is expected to be an ISO 8601 time string
   * representing the start of the 100-year period.
   */
  def setCenturyStart(start: String): TimeFormat = {
    val date = new Date(TimeFormat.parseIso(start))
    sdf.set2DigitYearStart(date)
    this
  }

  /**
   * Overrides toString to return the format string.
   */
  override def toString: String = format
}

//==============================================================================

object TimeFormat {

  def apply(format: String) = new TimeFormat(format)

  /**
   * Provides a TimeFormat for the default ISO 8601 format.
   */
  val Iso = TimeFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

  /**
   * Represents the given time in milliseconds as the
   * default ISO 8601 time format.
   */
  def formatIso(millis: Long): String = Iso.format(millis)

  /**
   * Provides a convenience method for parsing an arbitrary
   * ISO 8601 time string. If this needs to be applied numerous times,
   * e.g. for each sample of a dataset, it is recommended that a
   * TimeFormat object be created with fromIsoValue and reused.
   */
  def parseIso(time: String): Long = fromIsoValue(time).parse(time)

  /**
   * Creates a TimeFormat from an ISO time value.
   * This supports most common ISO 8601 representations
   * not including weeks or time zones other than "Z".
   */
  def fromIsoValue(value: String): TimeFormat = {
    val format = value.split("T") match {
      case Array(date) =>
        // No time component
        getDateFormatString(date)
      case Array(date, time) =>
        List(
          getDateFormatString(date),
          getTimeFormatString(time)
        ).mkString("'T'")
    }
    TimeFormat(format)
  }

  /**
   * Matches the date portion of an ISO time to a format string.
   */
  private def getDateFormatString(s: String): String = s.length match {
    case 4 => "yyyy"
    case 6 => "yyMMdd" //Note, yyyyMM is not ISO compliant
    case 7 => {
      if (s.contains("-")) "yyyy-MM"
      else "yyyyDDD"
    }
    case 8 => {
      if (s.contains("-")) "yyyy-DDD"
      else "yyyyMMdd"
    }
    case 10 => "yyyy-MM-dd"
    case _ => throw new IllegalArgumentException("Failed to determine a date format for " + s)
  }

  /**
   * Matches the time portion of an ISO time to a format string.
   */
  private def getTimeFormatString(s: String): String = {
    // Handle the "Z" time zone designation
    val length = s.indexOf("Z") match {
      case n: Int if (n != -1) => n
      case _ => s.length
    }

    length match {
      case 0  => ""
      case 2  => "HH"
      case 4  => "HHmm"
      case 5  => "HH:mm"
      case 6  => "HHmmss"
      case 8  => "HH:mm:ss"
      case 12 => "HH:mm:ss.SSS"
      case _ => throw new IllegalArgumentException("Failed to determine a time format for " + s)
    }
  }

}
