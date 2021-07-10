package latis.model

import scala.annotation.unused

import cats.syntax.all._

import latis.data.Data
import latis.data.NullData
import latis.metadata.Metadata
import latis.time.Time
import latis.units.MeasurementScale
import latis.util.Identifier
import latis.util.LatisException

trait ScalarFactory {

  def apply(id: Identifier, valueType: ValueType): Scalar = {
    val md = Metadata("id" -> id.asString, "type" -> valueType.toString)
    new Scalar(md, id, valueType)
  }

  def fromMetadata(metadata: Metadata): Either[LatisException, Scalar] =
    metadata.getProperty("class").map {
      //TODO: construct dynamically? ReflectionUtils.callMethodOnCompanionObject(cls, fromMetadata, md)
      case "latis.model.Index" => getId(metadata).map(Index(_))
      case "latis.time.Time"   => Time.fromMetadata(metadata)
      case cls                 => Left(LatisException(s"Scalar class not found: $cls"))
    }.getOrElse {
      for {
        id        <- getId(metadata)
        valueType <- getValueType(metadata)
        units     <- getUnits(metadata)
        scale     <- getScale(metadata)
        missValue <- getMissingValue(metadata, valueType)
        fillValue <- getFillValue(metadata, valueType)
        precision <- getPrecision(metadata, valueType)
        ascending <- getAscending(metadata)
      } yield new Scalar(
        metadata,
        id,
        valueType,
        units = units,
        scale = scale,
        missingValue = missValue,
        fillValue = fillValue,
        precision = precision,
        ascending = ascending
      )
    } //TODO: other properties


  protected def getId(metadata: Metadata): Either[LatisException, Identifier] =
    metadata.getProperty("id")
      .toRight(LatisException("No id defined"))
      .flatMap(id => Identifier.fromString(id).toRight(LatisException(s"Invalid id: $id")))

  protected def getValueType(metadata: Metadata): Either[LatisException, ValueType] =
    metadata.getProperty("type")
      .toRight(LatisException("No type defined"))
      .flatMap(ValueType.fromName)

  protected def getUnits(metadata: Metadata): Either[LatisException, Option[String]] =
    metadata.getProperty("units").traverse(_.asRight)

  protected def getScale(@unused metadata: Metadata): Either[LatisException, Option[MeasurementScale]] =
    None.asRight //only supported for Time, so far

  protected def getMissingValue(metadata: Metadata, valueType: ValueType): Either[LatisException, Option[Data]] =
    metadata.getProperty("missingValue").traverse { mv =>
      if (mv == "null") NullData.asRight
      else valueType.parseValue(mv)
    }

  protected def getFillValue(metadata: Metadata, valueType: ValueType): Either[LatisException, Option[Data]] =
    metadata.getProperty("fillValue").traverse { fv =>
      if (fv == "null") LatisException("FillValue must not be 'null'").asLeft
      else valueType.parseValue(fv)
    }

  protected def getPrecision(metadata: Metadata,  valueType: ValueType): Either[LatisException, Option[Int]] =
    metadata.getProperty("precision").traverse { s =>
      Either.catchOnly[NumberFormatException](s.toInt)
        .leftMap(_ => LatisException("Precision must be an integer"))
        .flatMap { p =>
          if (! List(FloatValueType, DoubleValueType, BigDecimalValueType).contains(valueType))
            LatisException(s"Precision is not supported for value type: $valueType").asLeft
          else if (p < 0) LatisException("Precision must not be negative").asLeft
          else p.asRight
        }
    }.leftMap(ex => LatisException(ex))

  protected def getAscending(metadata: Metadata): Either[LatisException, Boolean] =
    metadata.getProperty("order", "asc").toLowerCase match {
      case "asc" => true.asRight
      case "des" => false.asRight
      case _     => LatisException("Order must be 'asc' or 'des'").asLeft
    }
}
