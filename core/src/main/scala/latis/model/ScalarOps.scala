package latis.model

import cats.syntax.all._

import latis.data._
import latis.util.DefaultDatumOrdering
import latis.util.LatisException

class ScalarOps(scalar: Scalar) {

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
   * Returns a string representation of the given Data based on this DataType.
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
   * Converts a string value into the appropriate type and units
   * for this Scalar.
   */
  def convertValue(value: String): Either[Exception, Datum] =
    parseValue(value)
  //TODO: support units, e.g. "1.2 meters"

  /**
   * Defines a PartialOrdering for Datums of the type described by this Scalar.
   * The "order" metadata property can be set to "asc" (default) or "des" for
   * descending.
   */
  def ordering: PartialOrdering[Datum] =
    if (scalar.ascending) DefaultDatumOrdering
    else DefaultDatumOrdering.reverse

}
