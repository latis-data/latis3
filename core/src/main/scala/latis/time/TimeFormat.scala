package latis.time

import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.TimeZone

import scala.util._

/**
 * TimeFormat support that is thread safe and assumes GMT time zone.
 */
class TimeFormat(format: String) {

  private val sdf: SimpleDateFormat = {
    val sdf = try {
      new SimpleDateFormat(format, Locale.ENGLISH)
    } catch {
      case e: Exception =>
        val msg = s"Could not parse '$format' as a time format."
        throw new IllegalArgumentException(msg, e)
    }
    sdf.setTimeZone(TimeZone.getTimeZone("GMT"))
    sdf
  }

  def format(millis: Long): String = this.synchronized {
    sdf.format(new Date(millis))
  }

  def parse(string: String): Either[Exception, Long] = this.synchronized {
    Try {
      sdf.parse(string).getTime
    } match {
      case Success(v) => Right(v)
      case Failure(t) =>
        val msg = s"Could not parse '$string' with the format $format"
        Left(new IllegalArgumentException(msg, t))
    }
  }

  /**
   * Sets the 100-year period to be used to interpret 2-digit years.
   * This time is expected to be an ISO 8601 time string
   * representing the start of the 100-year period.
   */
  //TODO: can this be passed in as an optional constructor arg?
  def setCenturyStart(start: String): TimeFormat = {
    TimeFormat.parseIso(start) match {
      case Right(t) =>
        sdf.set2DigitYearStart(new Date(t))
      case Left(e) =>
        throw e
    }
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
  val Iso: TimeFormat = TimeFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

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
  def parseIso(time: String): Either[Exception, Long] =
    for {
      formatter <- fromIsoValue(time)
      tvalue    <- formatter.parse(time)
    } yield tvalue

  /**
   * Creates a TimeFormat from an ISO time value.
   * This supports most common ISO 8601 representations
   * not including weeks or time zones other than "Z".
   */
  def fromIsoValue(value: String): Either[Exception, TimeFormat] = {
    val format = value.split("T") match {
      case Array(date) =>
        // No time component
        getDateFormatString(date)
      case Array(date, time) =>
        for {
          d <- getDateFormatString(date)
          t <- getTimeFormatString(time)
        } yield List(d, t).mkString("'T'")
    }
    format.map {
      TimeFormat(_)
    }
    //TODO: combine error messages if both time and date part fail
  }

  /**
   * Matches the date portion of an ISO time to a format string.
   */
  private def getDateFormatString(s: String): Either[Exception, String] =
    s.length match {
      case 4 => Right("yyyy")
      case 6 => Right("yyMMdd") //Note, yyyyMM is not ISO compliant
      case 7 =>
        if (s.contains("-")) Right("yyyy-MM")
        else Right("yyyyDDD")
      case 8 =>
        if (s.contains("-")) Right("yyyy-DDD")
        else Right("yyyyMMdd")
      case 10 => Right("yyyy-MM-dd")
      case _ =>
        val msg = s"Failed to determine a date format for $s"
        Left(new IllegalArgumentException(msg))
    }

  /**
   * Matches the time portion of an ISO time to a format string.
   */
  private def getTimeFormatString(s: String): Either[Exception, String] = {
    // Handle the "Z" time zone designation
    val length = s.indexOf("Z") match {
      case n: Int if (n != -1) => n
      case _                   => s.length
    }

    length match {
      case 0  => Right("")
      case 2  => Right("HH")
      case 4  => Right("HHmm")
      case 5  => Right("HH:mm")
      case 6  => Right("HHmmss")
      case 8  => Right("HH:mm:ss")
      case 12 => Right("HH:mm:ss.SSS")
      case _ =>
        val msg = s"Failed to determine a time format for $s"
        Left(new IllegalArgumentException(msg))
    }
  }

}
