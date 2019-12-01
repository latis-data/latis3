package latis.util

import cats.implicits._
import java.net.URI
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.Schema
import javax.xml.validation.SchemaFactory

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
   * Finds the schemaLocation definition in the given FDML.
   * It is only required in order to support validation.
   */
  def getSchemaLocation(fdmlUri: URI): Either[LatisException, URI] = {
    val pattern = """.*noNamespaceSchemaLocation\s*=\s*"(.*?)".*""".r
    NetUtils.readUriIntoString(fdmlUri).flatMap { xml =>
      val z = xml.replaceAll("\n", " ")
      z match { //pattern match doesn't like the new lines
        case pattern(uri) =>
          NetUtils.resolveUri(uri)
        case _ =>
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
    isValid.leftMap(
      t => LatisException(s"Validation failed for $fdmlUri\n$t", t)
    )
    // Note, the validation Exception's toString provides line numbers that the message doesn't.
  }

  /**
   * Validates FDML represented by the given URI.
   */
  def validateFdml(uri: String): Either[LatisException, Unit] =
    NetUtils.parseUri(uri).flatMap(validateFdml)
}
