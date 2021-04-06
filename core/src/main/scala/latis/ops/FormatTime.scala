package latis.ops

import cats.syntax.all._

import latis.data._
import latis.model.DataType
import latis.model.StringValueType
import latis.time.Time
import latis.time.TimeFormat
import latis.time.TimeScale
import latis.units.UnitConverter
import latis.util.LatisException

/**
 * Operation to format all time variables using the given TimeFormat.
 * Instead of matching specific Identifiers, this will apply to
 * all Scalars of class Time.
 */
case class FormatTime(format: TimeFormat) extends TimeOperation {
  /**
   * Defines a data value converter based on the Time variable's value type.
   */
  def makeConverter(t: Time): Datum => Datum =
    // Note that the resulting function definition is pushed down
    // to minimize the amount of work it needs to do since it will
    // be applied to every Sample.
    t.valueType match {
      case StringValueType =>
        //TODO: no-op if format does not change
        val fmt = t.timeFormat.get //bug if string time has no format
        (d: Datum) =>
          d match {
            case Text(s) =>
              fmt
                .parse(s)
                .map(format.format(_))
                .flatMap(Data.fromValue(_))
                .fold(throw _, identity)
            case _ => throw new LatisException(s"Data does not match string value type: $d")
          }
      case _ =>
        // Define the numeric unit converter to the default TimeScale
        //TODO: use unit converter only if needed
        val converter = UnitConverter(t.timeScale, TimeScale.Default)
        (d: Datum) =>
          d match {
            case Number(d) =>
              Data.fromValue(format.format(converter.convert(d).toLong)).fold(throw _, identity)
            case _ => throw new LatisException(s"Data is not a numeric value type: $d")
          }
    }

  /**
   * Updates the units and type metadata for time variables.
   */
  def applyToModel(model: DataType): Either[LatisException, DataType] =
    model.map {
      case t: Time => Time(t.metadata + ("units" -> format.toString) + ("type" -> "string"))
      case dt      => dt
    }.asRight
}

object FormatTime {
  def fromArgs(args: List[String]): Either[LatisException, FormatTime] = args match {
    case format :: Nil =>
      Either
        .catchNonFatal(FormatTime(TimeFormat(format)))
        .leftMap(LatisException(_))
    case _ => Left(LatisException("FormatTime requires one argument"))
  }
}
