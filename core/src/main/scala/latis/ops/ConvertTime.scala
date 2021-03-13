package latis.ops

import cats.syntax.all._

import latis.data._
import latis.model.DataType
import latis.model.StringValueType
import latis.time.Time
import latis.time.TimeScale
import latis.units.UnitConverter
import latis.util.LatisException

/**
 * Operation to convert all time variables to the given TimeScale.
 * Instead of matching specific Identifiers, this will apply to
 * all Scalars of class Time.
 */
case class ConvertTime(scale: TimeScale) extends TimeOperation {

  /**
   * Defines a data value converter based on the Time variable's value type.
   */
  def makeConverter(t: Time): Datum => Datum = {
    // Note that the resulting function definition is pushed down
    // to minimize the amount of work it needs to do since it will
    // be applied to every Sample.

    // Define the numeric unit converter
    val converter = UnitConverter(t.timeScale, scale)

    // Add special handling for formatted times.
    t.valueType match {
      case StringValueType =>
        val format = t.timeFormat.get //bug if string time has no format
        (d : Datum) => d match {
          case Text(s) =>
            format.parse(s)
              .map(converter.convert(_))
              .flatMap(Data.fromValue(_))
              .fold(throw _, identity)
          case _ => throw new LatisException(s"Data does not match string value type: $d")
        }
      case _ =>
        (d : Datum) => d match {
          case Number(d) =>
            Data.fromValue(converter.convert(d)).fold(throw _, identity)
          case _ => throw new LatisException(s"Data is not a numeric value type: $d")
        }
    }
  }

  /**
   * Updates the units and type metadata for time variables.
   */
  def applyToModel(model: DataType): Either[LatisException, DataType] =
    model.map {
      case t: Time => Time(t.metadata + ("units" -> scale.toString) + ("type" -> "double"))
      case dt => dt
    }.asRight
}

object ConvertTime {
  def fromArgs(args: List[String]): Either[LatisException, ConvertTime] = args match {
    case units :: Nil => Either.catchNonFatal(ConvertTime(TimeScale(units)))
      .leftMap(LatisException(_))
    case _ => Left(LatisException("ConvertTime requires one argument"))
  }
}
