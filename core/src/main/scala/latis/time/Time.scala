package latis.time

import latis.data.Data
import latis.data.Datum
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
    case _ => None
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
   * Overrides the basic Scalar Ordering to provide
   * support for formatted time strings.
   */
  override def ordering: Ordering[Datum] = _ordering

  // Optimization to define the Ordering only once.
  private lazy val _ordering = timeFormat map { format =>
    new Ordering[Datum] {
      def compare(x: Datum, y: Datum): Int = (x, y) match {
        case (t1: Data.StringValue, t2: Data.StringValue) =>
          //TODO: invalid time value
          val z = for {
            v1 <- format.parse(t1.value)
            v2 <- format.parse(t2.value)
          } yield v1.compare(v2)
          z.getOrElse { ??? }
        case _ =>
          val msg = s"Incomparable data values: $x $y"
          throw new IllegalArgumentException(msg)
      }
    }
  } getOrElse {
    super.ordering //not formatted so treat like others
  }

  /**
   * Overrides value conversion to support formatted time strings.
   * This expects a numeric string or ISO format.
   */
  override def convertValue(value: String): Either[Exception, Datum] = {
    TimeFormat.parseIso(value).map { time =>  //time in default units
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

}

object Time {
  
  def apply(md: Metadata) = new Time(md)
}
