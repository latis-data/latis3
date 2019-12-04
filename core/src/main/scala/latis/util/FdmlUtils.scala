package latis.util

import java.net.URI
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.Schema
import javax.xml.validation.SchemaFactory

import cats.implicits._
import org.xml.sax.SAXParseException

object FdmlUtils {

  /** Returns a StreamSource for the given XML URI. */
  def getXmlSource(uri: URI): Either[LatisException, StreamSource] =
    NetUtils.resolveUri(uri).flatMap { u =>
      Either.catchNonFatal {
        new StreamSource(u.toURL.openStream)
      }.leftMap(LatisException(s"Failed to read XML: $uri", _))
    }

  /** Constructs a Schema from the given URI. */
  def getSchema(uri: URI): Either[LatisException, Schema] =
    NetUtils.resolveUri(uri).flatMap { u =>
      Either.catchNonFatal {
        SchemaFactory
          .newInstance("http://www.w3.org/2001/XMLSchema")
          .newSchema(new StreamSource(u.toURL.openStream)) /////////////use getXmlSource
      }.leftMap(LatisException(s"Failed to read schema: $uri", _))
    }

  /**
   * Constructs a Schema from the schemaLocation in the given FDML.
   */
  def getSchemaFromFdml(fdmlUri: URI): Either[LatisException, Schema] =
    getSchemaLocation(fdmlUri).flatMap(getSchema)

  /**
   * Finds the schema location definition in the given FDML.
   * It is only required in order to support validation.
   */
  def getSchemaLocation(fdmlUri: URI): Either[LatisException, URI] = {
    val pattern = """noNamespaceSchemaLocation\s*=\s*"(.*?)"""".r
    NetUtils.readUriIntoString(fdmlUri).flatMap { xml =>
      pattern.findFirstMatchIn(xml) match {
        case Some(m) =>
          val uri = m.group(1)
          NetUtils.resolveUri(uri)
        case None =>
          Either.left(LatisException(s"Schema location not defined in $fdmlUri"))
      }
    }
  }

  /**
   * Validates the given FDML.
   */
  def validateFdml(fdmlUri: URI): Either[LatisException, Unit] = {
    val isValid = for {
      schema <- getSchemaFromFdml(fdmlUri)
      fdml   <- getXmlSource(fdmlUri) //TODO: avoid reading fdml twice? reuse xml StreamSource?
      valid  <- Either.catchNonFatal(schema.newValidator().validate(fdml))
    } yield valid
    isValid.leftMap { t =>
      val baseMessage = s"Validation failed for $fdmlUri"
      t match {
        case spe: SAXParseException =>
          val msg = baseMessage +
            System.lineSeparator +
            s"  at line ${spe.getLineNumber}, column ${spe.getColumnNumber}" +
            System.lineSeparator +
            "  " + spe.getMessage
          LatisException(msg, spe)
        case le: LatisException => le
        case _                  => LatisException(cause = t)
      }
    }
    // Note, the validation Exception's toString provides line numbers that the message doesn't.
  }

  /**
   * Validates FDML represented by the given URI.
   */
  def validateFdml(uri: String): Either[LatisException, Unit] =
    NetUtils.parseUri(uri).flatMap(validateFdml)
}
