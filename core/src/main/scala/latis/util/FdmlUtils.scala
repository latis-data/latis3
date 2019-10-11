package latis.util

import javax.xml._
import java.nio.file._
import javax.xml.validation.SchemaFactory
import javax.xml.transform.stream.StreamSource
import scala.util.Try
import scala.util.Right
import scala.util.Success
import scala.util.Failure
import java.net.URI
import javax.xml.validation.Schema

object FdmlUtils {

  /**
   * Optionally return a URI for an FDML file given a Dataset identifier.
   */
  def resolveFdml(uri: URI): Option[URI] = {
    if (uri.isAbsolute) Some(uri)
    else {
      val dir = LatisConfig.getOrElse("latis.fdml.dir", "datasets")
      //TODO: look in home? $LATIS_HOME?
      FileUtils.resolveUri(Paths.get(dir, uri.getPath).toString)
      // Note on use of toString:
      // Path.toUri prepends the home directory as the base URI
    }
  }

  def resolveFdml(uri: String): Option[URI] =
    resolveFdml(new URI(uri))

  /**
   * Load the FDML XML schema.
   */
  lazy val schema: Schema = {
    val schemaSource: StreamSource = {
      val uri: URI = FileUtils.resolveUri("fdml.xsd") getOrElse {
        throw new RuntimeException("Failed to find the FDML schema file.")
      }
      new StreamSource(uri.toURL.openStream) //TODO: handle errors
    }
    SchemaFactory
      .newInstance("http://www.w3.org/2001/XMLSchema")
      .newSchema(schemaSource)
  }

  def validateFdml(fdmlUri: URI): Either[String, Unit] =
    Try {
      val uri: URI = resolveFdml(fdmlUri) getOrElse {
        throw new RuntimeException(s"Failed to find the FDML file: $fdmlUri")
      }
      val source = new StreamSource(uri.toURL.openStream)
      schema.newValidator().validate(source)
    } match {
      case Success(v) => Right(())
      case Failure(e) => Left(e.toString)
      //Note, toString provides line numbers not included in the message
    }

  def validateFdml(uri: String): Either[String, Unit] =
    validateFdml(new URI(uri))
}
