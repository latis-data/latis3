package latis.time

import cats.syntax.all._

import latis.data.Data
import latis.data.Datum
import latis.data.Text
import latis.metadata.Metadata
import latis.model.Scalar
import latis.model.StringValueType
import latis.model.ValueType
import latis.units.UnitConverter
import latis.util.Identifier
import latis.util.LatisException

/**
 * Time is a Scalar that provides special behavior for formated time values.
 */
//class Time(metadata: Metadata) extends Scalar(metadata) {
  //TODO: make sure this has the id or alias "time"
  //TODO: validate units eagerly

class Time(id: Identifier, valueType: ValueType) extends Scalar(id, valueType) {

  /** Override to preserve type */
  //override def rename(id: Identifier): Time = Time(metadata + ("id" -> id.asString))

  /**
   * Returns the units from the metadata.
   */
  val units: String = metadata.getProperty("units").getOrElse {
    val msg = "A Time variable must have units."
    throw new RuntimeException(msg)
  }

  /**
   * Constructs Some TimeFormat if the time is represented
   * as a string. Otherwise returns None.
   */
  val timeFormat: Option[TimeFormat] = valueType match {
    case StringValueType => TimeFormat.fromExpression(units).toOption
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
    else TimeScale.fromExpression(units).fold(throw _, identity)

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
   *
   * This will try to interpret the value in this Time's units (numeric or format)
   * or then as an ISO string.
   *
   * This method is intended for lightweight use such as parsing time selections.
   * Construct a reusable TimeFormat or UnitConverter for bigger conversion tasks.
   */
  override def convertValue(value: String): Either[Exception, Datum] =
    (valueType match {
      case StringValueType =>
        //Try to match this Time's format or else ISO
        val format = timeFormat.get //safe since this has type string
        format.parse(value)
          .recoverWith(_ => TimeFormat.parseIso(value))
          .map(t => Data.StringValue(format.format(t)))
      case _ =>
        //Try to interpret as this value type then as ISO.
        super.parseValue(value)
          .recoverWith { _ =>
            TimeFormat.parseIso(value).flatMap { t =>
              val t2 = UnitConverter(TimeScale.Default, timeScale)
                .convert(t.toDouble)
              Either.fromOption(
                valueType.convertDouble(t2),
                LatisException(s"Failed to convert time value: $value")
              )
            }
          }
    }).leftMap { le =>
      LatisException(s"Failed to interpret time value: $value", le)
    }

}

object Time {

  def apply(md: Metadata) = new Time(md)
}
