package latis.ops

import cats.syntax.all._
import scodec.bits.ByteVector

import latis.data._
import latis.model._
import latis.ops.ConvertBinary._
import latis.util.Identifier
import latis.util.LatisException

/**
 * The ConvertBinary operation converts the target binary variable to a
 * string variable representing the given base.
 *
 * The new variable will have the same identifier but the type will be changed
 * to "string" and the encoding will be appended to the mediaType, if defined.
 *
 * This currently supports bases 2, 16, and 64.
 */
case class ConvertBinary(id: Identifier, base: Base) extends MapOperation {
  //TODO: ensure that id maps to a binary variable
  //TODO: support variable in nested function
  //TODO: modify other metadata?

  /** Converts the binary data to a string for the given base. */
  private val convert: Array[Byte] => Datum = base match {
    case Base2  => bytes => Data.StringValue(ByteVector(bytes).toBin)
    case Base16 => bytes => Data.StringValue(ByteVector(bytes).toHex)
    case Base64 => bytes => Data.StringValue(ByteVector(bytes).toBase64)
  }

  /** Replaces the binary value with the converted string. */
  def mapFunction(model: DataType): Sample => Sample = {
    val pos = model.findPath(id) match {
      case Some(head :: Nil) => head
      case Some(_) => throw LatisException("Target variable must not be in nested function")
      case None    => throw LatisException("Variable not found")
    }
    (sample: Sample) => sample.getValue(pos) match {
      case Some(b: Data.BinaryValue) => sample.updatedValue(pos, convert(b.value))
      case _ => throw LatisException("Target variable must be Binary")
    }
  }

  /** Changes the Binary variable to a String variable. */
  def applyToModel(model: DataType): Either[LatisException, DataType] =
    model.map {
      case s: Scalar if (s.id == id) =>
        var md = s.metadata + ("type", StringValueType.toString)
        s.metadata.getProperty("mediaType").foreach { mt =>
          md = md + ("mediaType", s"$mt;$base") //append encoding to mediaType
        }
        Scalar.fromMetadata(md).fold(throw _, identity) //should be safe
      case dt => dt
    }.asRight
}

object ConvertBinary {

  def fromArgs(args: List[String]): Either[LatisException, ConvertBinary] = args match {
    case id :: base :: Nil => for {
      id   <- Either.fromOption(Identifier.fromString(id), LatisException("Invalid Identifier"))
      base <- base match {
        case "2"  => Base2.asRight
        case "16" => Base16.asRight
        case "64" => Base64.asRight
        case _    => LatisException("ConvertBinary base must be 2, 16, or 64").asLeft
      }
    } yield ConvertBinary(id, base)
    case _ => LatisException("ConvertBinary requires two arguments: id, base").asLeft
  }

  sealed trait Base
  object Base2  extends Base { override def toString: String = "base2" }
  object Base16 extends Base { override def toString: String = "base16" }
  object Base64 extends Base { override def toString: String = "base64" }
}
