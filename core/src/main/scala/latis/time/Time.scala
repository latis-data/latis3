package latis.time

import latis.data.Data
import latis.data.Datum
import latis.metadata.Metadata
import latis.model.Scalar

/**
 * Time is a Scalar that provides special behavior for formated time values.
 */
class Time(metadata: Metadata) extends Scalar(metadata) {
  //TODO: make sure this has the id or alias "time"

  /**
   * Constructs a TimeFormat if the time is represented as a string.
   */
  val timeFormat: Option[TimeFormat] = this("type") match {
    case Some("string") =>
      this("units") match {
        case Some(f) =>
          Some(TimeFormat(f)) //TODO: error handling, invalid format
        case None =>
          val msg = "A Time variable must have units."
          throw new RuntimeException(msg)
      }
    case _ => None
  }

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
          format.parse(t1.value) compare format.parse(t2.value)
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
  override def convertValue(value: String): Either[Exception, Datum] =
    timeFormat map { format =>
      val time = TimeFormat.parseIso(value) //TODO: error if not ISO
      Right(Data.StringValue(format.format(time)))
    } getOrElse {
      super.convertValue(value) //treat like any other value
    }

}

object Time {
  
  def apply(md: Metadata) = new Time(md)
}
