package latis.time

import cats.syntax.all._

import latis.data.Data
import latis.data.Datum
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

  /** Override to preserve type */
  @deprecated
  override def rename(id: Identifier): Time =
    Time.fromMetadata(metadata + ("id" -> id.asString)).fold(throw _, identity)

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

object Time extends ScalarFactory {

  /*
  TODO: convenient constructor, for testing only?
    must be consistent
    various flavors? fromUnits, ...?
  TODO: separable Time API? squants?
   */
//  def apply(
//    id: Identifier = id"time",
//    valueType: ValueType = LongValueType,
//    units: String = "milliseconds since 1970-01-01",
//    scale: TimeScale = TimeScale.Default,
//    timeFormat: Option[TimeFormat] = None
//  ): Time = new Time(
//    Metadata(
//      "id" -> id.asString,
//      "valueType" -> valueType.toString),
//    id, valueType
//  )


  override def fromMetadata(metadata: Metadata): Either[LatisException, Time] = {
    if (metadata.getProperty("class").contains("latis.time.Time")) {
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
        metadata,
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
    } else LatisException("Time metadata has wrong class").asLeft
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
