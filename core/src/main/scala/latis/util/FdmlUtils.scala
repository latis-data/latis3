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
  def resolveFdmlUri(file: String): Option[URI] = {
    val dir = LatisConfig.getOrElse("latis.fsml.dir", "datasets")
    //TODO: look in home? $LATIS_HOME?
    FileUtils.resolveUri(Paths.get(dir, file).toString)
    // Note on use of toString:
    // Path.toUri prepends the home directory as the base URI
  }
  
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
      
  def validateFdml(fdmlFile: String): Either[String, Unit] = Try {
    val uri: URI = resolveFdmlUri(fdmlFile) getOrElse {
      throw new RuntimeException(s"Failed to find the FDML file: $fdmlFile")
    }
    val source = new StreamSource(uri.toURL.openStream)
    schema.newValidator().validate(source)
  } match {
    case Success(v) => Right(())
    case Failure(e) => Left(e.toString) 
    //Note, toString provides line numbers not included in the message
  }
}
