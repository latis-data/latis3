package latis.model

import cats.syntax.all.*

import latis.data.*
import latis.util.DefaultDatumOrdering
import latis.util.LatisException
import latis.util.StringUtils

trait ScalarAlgebra { scalar: Scalar =>

  /** Specifies if fillData can be used to replace invalid data. */
  def isFillable: Boolean = scalar.fillValue.nonEmpty

  /**
   * Converts a string value into the appropriate Datum type for this Scalar.
   *
   * If the value fails to parse and a fillValue is defined, the resulting
   * Datum will encode that fill value. Because this returns a Datum,
   * NullData ("null") may not be used for fillValue metadata.
   * Note that NullData may be used for Scala fillData in other contexts.
   */
  def parseValue(value: String): Either[LatisException, Datum] =
    scalar.valueType.parseValue(value).recoverWith { ex =>
      if (isFillable) scalar.fillData match {
        // Make sure fill value is not NullData
        case fv: Datum => fv.asRight
        case fv => LatisException(s"Invalid fillValue: $fv").asLeft
      } else {
        ex.asLeft
      }
    }

  /**
   * Returns a string representation of the given Data based on this Scalar.
   *
   * This should be used to get a string representation of a Datum
   * instead of Datum.asString so properties of the Scalar, such as
   * precision, can be applied to an otherwise agnostic data value.
   */
  def formatValue(data: Data): String =
    (data, scalar.precision) match {
      case (Real(d), Some(p)) => (s"%.${p}f").format(d)
      case (d: Datum, _)      => d.asString
      case (NullData, _)      => "null"
      case _ => "error" //Tuple and Function data should not show up here
    }

  /**
   * Returns a Double representation of the given Data based on this Scalar.
   *
   * Note that the default numeric type (e.g. for math) is Double.
   *
   * This assumes that the given data is a valid member of this Scalar's
   * ValueType. Invalid data will result in a NaN.
   */
  def valueAsDouble(data: Data): Double = data match {
    case Number(v) => v
    case _         => Double.NaN
  }

  /**
   * Converts a string value into the appropriate type and units
   * for this Scalar.
   */
  def convertValue(value: String): Either[LatisException, Datum] = valueType match {
    case StringValueType => parseValue(StringUtils.removeDoubleQuotes(value))
    case _               => parseValue(value)
  }

  /**
   * Defines a PartialOrdering for Datums of the type described by this Scalar.
   * The "order" metadata property can be set to "asc" (default) or "des" for
   * descending.
   */
  def ordering: PartialOrdering[Datum] =
    if (scalar.ascending) DefaultDatumOrdering
    else DefaultDatumOrdering.reverse

  /** Returns the numerical cadence as optionally defined in the metadata. */
  def getCadence: Option[Double] =
    metadata.getProperty("cadence").flatMap(_.toDoubleOption)

  /** Returns the numeric coverage as optionally defined in the metadata. */
  def getCoverage: Option[(Double,Double)] = {
    metadata.getProperty("coverage").flatMap { c =>
      c.split("/").toList match {
        case s :: e :: Nil if s.nonEmpty && e.nonEmpty =>
          for {
            s <- s.toDoubleOption
            e <- e.toDoubleOption
          } yield (s, e)
        case _ => None
      }
    }
  }
  
  /** 
   * Determines if this Scalar represents binned (not instantaneous) values. 
   * 
   * This is currently based on the existence of the `binWidth` property.
   * This may evolve as additional bin semantics are supported.
   */
  def isBinned: Boolean = binWidth.nonEmpty

  /** Defines the string representation as the Scalar id. */
  override def toString: String = id.asString
}
