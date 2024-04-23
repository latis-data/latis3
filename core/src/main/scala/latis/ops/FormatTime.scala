package latis.ops

import java.lang.Math.*

import cats.syntax.all.*

import latis.data.*
import latis.model.*
import latis.time.Time
import latis.time.TimeConverter
import latis.time.TimeFormat
import latis.time.TimeScale
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
  def makeConverter(t: Time): Datum => Datum = {
    // Note that the resulting function definition is pushed down
    // to minimize the amount of work it needs to do since it will
    // be applied to every Sample.

    t.valueType match {
      case StringValueType =>
        //TODO: no-op if format does not change
        val fmt = t.timeFormat.get //bug if string time has no format
        (d : Datum) => d match {
          case Text(s) =>
            fmt.parse(s)
              .map(format.format)
              .flatMap(Data.fromValue)
              .fold(throw _, identity)
          case _ => throw LatisException(s"Data does not match string value type: $d")
        }
      case _: NumericType =>
        // Define the numeric unit converter to the default TimeScale
        val converter = TimeConverter(t.timeScale, TimeScale.Default)
        (d: Datum) => d match {
          case Number(d) =>
            Data.fromValue(format.format(round(converter.convert(d)))).fold(throw _, identity)
          case _ => throw LatisException(s"Data is not a numeric value type: $d")
        }
      case t => throw LatisException(s"Invalid type for Time: $t") //bug
    }
  }

  /**
   * Updates the units and type metadata for time variables.
   */
  def applyToModel(model: DataType): Either[LatisException, DataType] =
    model.map {
      case t: Time =>
        Time.fromMetadata( t.metadata + ("units" -> format.toString) + ("type" -> "string"))
          .fold(throw _, identity)
      case dt => dt
    }.asRight
}

object FormatTime {

  def builder: OperationBuilder = (args: List[String]) => fromArgs(args)

  def fromArgs(args: List[String]): Either[LatisException, FormatTime] = args match {
    case format :: Nil =>
      TimeFormat.fromExpression(format).map(FormatTime(_))
    case _ => LatisException("FormatTime requires one argument").asLeft
  }
}
