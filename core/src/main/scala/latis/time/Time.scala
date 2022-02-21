package latis.time

import cats.syntax.all._

import latis.data.Data
import latis.data.Datum
import latis.data.Text
import latis.metadata.Metadata
import latis.model.Scalar
import latis.model.ScalarFactory
import latis.model.StringValueType
import latis.model.ValueType
import latis.units.MeasurementScale
import latis.units.UnitConverter
import latis.util.Identifier
import latis.util.LatisException

/**
 * Time is a Scalar that provides special behavior for time values.
 */
class Time protected (
  metadata: Metadata,
  id: Identifier,
  valueType: ValueType,
  units: Option[String],
  scale: Option[MeasurementScale],
  val timeFormat: Option[TimeFormat],
  missingValue: Option[Data] = None,
  fillValue: Option[Data] = None,
  precision: Option[Int] = None,
  ascending: Boolean = true
) extends Scalar(
  metadata,
  id,
  valueType,
  units = units,
  scale = scale,
  missingValue = missingValue,
  fillValue = fillValue,
  precision = precision,
  ascending = ascending
) {

  /** Provides safe access to Time's MeasurementScale. */
  def timeScale: TimeScale = scale.get.asInstanceOf[TimeScale]

  /**
   * Overrides the basic Scalar PartialOrdering to provide
   * support for formatted time strings.
   */
  override def ordering: PartialOrdering[Datum] =
    timeFormat.map { format =>
      TimeOrdering.fromFormat(format)
    }.getOrElse {
      // Not a formatted time so delegate to super
      super.ordering
    }

  /**
   * Overrides value conversion to support formatted time strings.
   *
   * This will try to interpret the value as an ISO string before
   * resorting to parseValue which interprets the data in native units.
   *
   * This method is intended for lightweight use such as parsing time selections.
   * Construct a reusable TimeFormat or UnitConverter for bigger conversion tasks.
   */
  override def convertValue(value: String): Either[LatisException, Datum] = {
    TimeFormat.parseIso(value).flatMap { ms =>
      // Interpreted as ISO
      valueType match {
        case StringValueType =>
          // Convert to this Time's format
          val format = timeFormat.get //safe since this has type string
          Data.StringValue(format.format(ms)).asRight
        case _ =>
          // Convert to this Time's numeric units
          val t = UnitConverter(TimeScale.Default, timeScale).convert(ms.toDouble)
          Either.fromOption(
            valueType.convertDouble(t),
            LatisException(s"Failed to convert time value: $value")
          )
      }
    }.recoverWith { _ =>
      // Parse assuming native units
      parseValue(value)
    }
  }

  /**
   * Overrides numeric conversion to handle formatted time strings.
   *
   * This assumes that the given data is valid for this Time.
   * Invalid data will result in a NaN.
   */
  override def valueAsDouble(data: Data): Double = valueType match {
    case StringValueType => data match {
      case Text(s) => timeFormat.get.parse(s).fold(_ => Double.NaN, _.toDouble)
      case _       => Double.NaN
    }
    case _ => super.valueAsDouble(data)
  }
}

object Time extends ScalarFactory {

  override def fromMetadata(metadata: Metadata): Either[LatisException, Time] = {
    for {
      id        <- getId(metadata)
      valueType <- getValueType(metadata)
      units     <- getUnits(metadata)
      reqUnits  <- getRequiredUnits(units)
      format    <- getTimeFormat(reqUnits, valueType)
      scale     <- getTimeScale(reqUnits, valueType)
      missValue <- getMissingValue(metadata, valueType)
      fillValue <- getFillValue(metadata, valueType)
      precision <- getPrecision(metadata, valueType)
      ascending <- getAscending(metadata)
    } yield new Time(
      metadata + ("class" -> "latis.time.Time"),
      id,
      valueType,
      units = units,
      scale = scale,
      timeFormat = format,
      missingValue = missValue,
      fillValue = fillValue,
      precision = precision,
      ascending = ascending
    )
  }

  /** Enforces that a Time variable has units defined. */
  protected def getRequiredUnits(units: Option[String]): Either[LatisException, String] =
    units.toRight(LatisException("Time requires units"))

  /**
   * Constructs a TimeScale based on units metadata.
   *
   * If the units represent a time format, this will have a string value type
   * and the default time scale will be used. Otherwise, a time scale will be
   * constructed from the numeric units.
   *
   * Although a time scale is required for Time scalars, this returns an Option
   * to match the default Scalar type signature.
   */
  protected def getTimeScale(units: String, valueType: ValueType): Either[LatisException, Option[MeasurementScale]] =
    valueType match {
      case StringValueType => TimeScale.Default.some.asRight
      case _               => TimeScale.fromExpression(units).map(_.some)
    }

  /**
   * Constructs a TimeFormat based on units metadata.
   *
   * Only Times with a string value type will have a time format.
   */
  protected def getTimeFormat(units: String, valueType: ValueType): Either[LatisException, Option[TimeFormat]] =
    valueType match {
      case StringValueType => TimeFormat.fromExpression(units).map(_.some)
      case _               => None.asRight
    }

}
