package latis.time

import latis.data.Data
import latis.data.Datum
import latis.data.Text
import latis.metadata.Metadata
import latis.model.Scalar
import latis.model.StringValueType
import latis.units.UnitConverter

/**
 * Time is a Scalar that provides special behavior for formated time values.
 */
class Time(metadata: Metadata) extends Scalar(metadata) {
  //TODO: make sure this has the id or alias "time"

  /**
   * Returns the units from the metadata.
   */
  val units: String = this("units").getOrElse {
    val msg = "A Time variable must have units."
    throw new RuntimeException(msg)
  }

  /**
   * Constructs Some TimeFormat if the time is represented
   * as a string. Otherwise returns None.
   */
  val timeFormat: Option[TimeFormat] = valueType match {
    case StringValueType => Option(TimeFormat(units))
    case _               => None
  }

  /**
   * Returns whether this Time represents a formatted
   * time string.
   */
  val isFormatted: Boolean = timeFormat.nonEmpty

  /**
   * Returns a TimeScale for use with time conversions.
   */
  val timeScale: TimeScale =
    if (isFormatted) TimeScale.Default
    else TimeScale(units)

  /**
   * Overrides the basic Scalar PartialOrdering to provide
   * support for formatted time strings.
   */
  override def ordering: PartialOrdering[Datum] =
    timeFormat.map { format =>
      new PartialOrdering[Datum] {
        // Note, None if data don't match our format
        def tryCompare(x: Datum, y: Datum): Option[Int] = (x, y) match {
          case (Text(t1), Text(t2)) =>
            val cmp = for {
              v1 <- format.parse(t1)
              v2 <- format.parse(t2)
            } yield Option(v1.compare(v2))
            cmp.getOrElse(None)
          case _ => None
        }

        def lteq(x: Datum, y: Datum): Boolean = (x, y) match {
          case (Text(t1), Text(t2)) =>
            val cmp = for {
              v1 <- format.parse(t1)
              v2 <- format.parse(t2)
            } yield v1 <= v2
            cmp.getOrElse(false)
          case _ => false
        }
      }
    }.getOrElse {
      // Not a formatted time so delegate to super
      super.ordering
    }

  /**
   * Overrides value conversion to support formatted time strings.
   * This expects a numeric string or ISO format.
   */
  override def convertValue(value: String): Either[Exception, Datum] =
    TimeFormat.parseIso(value).map { time => //time in default units
      timeFormat.map { format =>
        // this represents a formatted time string
        Data.StringValue(format.format(time))
      }.getOrElse {
        // this represents a numeric time value
        // Convert value to our TimeScale
        val t2 = UnitConverter(TimeScale.Default, timeScale)
          .convert(time.toDouble)
        valueType.convertDouble(t2).getOrElse {
          ??? //Bug: if this type supports unit conversions
          // then we should be able to convert back from a double
        }
      }
    }
}

object Time {
  def apply(md: Metadata) = new Time(md)
}
