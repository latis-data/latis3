package latis.input

import java.net.URI
import org.junit.Assert._
import org.junit.Ignore
import org.junit.Test
import org.scalatest.junit.JUnitSuite

import latis.data._
import latis.dataset.Dataset
import latis.ops._
import latis.util.FdmlUtils
import latis.util.StreamUtils


class TestFdmlReader extends JUnitSuite {

  @Test
  def validation(): Unit = {
    val fdmlFile = "datasets/data.fdml"
    FdmlUtils.validateFdml(fdmlFile) match {
      case Right(_) => //pass
      case Left(e) =>
        println(e.getMessage)
        fail
    }
  }

  @Test
  def validate_file_not_found(): Unit = {
    val fdmlFile = "nope.fdml"
    FdmlUtils.validateFdml(fdmlFile) match {
      case Left(e) =>
        assertTrue(e.getMessage.contains("Failed to read URI"))
    }
  }

  @Test
  def validate_on_load(): Unit = {
    try {
      FdmlReader(new URI("datasets/invalid.fdml"), true)
      fail("Validation did not work.")
    } catch {
      case e: Exception =>
        assertTrue(e.getMessage.contains("'{source}' is expected"))
    }
  }
  
  @Test
  def fdml_file(): Unit = {
    val ds = Dataset.fromName("data")
      .withOperation(Selection("time", ">=" , "2000-01-02"))
    StreamUtils.unsafeHead(ds.samples)match {
      case Sample(DomainData(Number(t)),RangeData(Integer(b), Real(c), Text(d))) =>
        assertEquals(1, t, 0)
        assertEquals(2, b)
        assertEquals(2.2, c, 0)
        assertEquals("b", d)
    }
  }

  @Test
  def match_schema_location_in_multiline_xml(): Unit = {
    val pattern = """.*noNamespaceSchemaLocation\s*=\s*"(.*?)".*""".r
    val xml = System.lineSeparator +  " foo " +
      """ xsi:noNamespaceSchemaLocation="http://latis-data.io/schemas/1.0/fdml.xsd"> """ +
      System.lineSeparator +  " bar "
    xml.replaceAll("\n", " ") match {   //pattern match doesn't like the new lines
      case pattern(uri) =>
        assertEquals("http://latis-data.io/schemas/1.0/fdml.xsd", uri)
    }
  }

  @Test @Ignore //TODO: avoid matching comments
  def match_first_schema_location_in_multiline_xml(): Unit = {
    val pattern = """.*noNamespaceSchemaLocation\s*=\s*"(.*?)".*""".r
    val xml = System.lineSeparator +  " foo " +
      """ xsi:noNamespaceSchemaLocation="http://latis-data.io/schemas/1.0/fdml.xsd"> """ +
      """ <!--xsi:noNamespaceSchemaLocation="not the second one"--> """ +
      System.lineSeparator +  " bar "
    xml.replaceAll("\n", " ") match {   //pattern match doesn't like the new lines
      case pattern(uri) =>
        assertEquals("http://latis-data.io/schemas/1.0/fdml.xsd", uri)
    }
  }

  @Test
  def get_schema_location(): Unit = {
    val uri = FdmlUtils.getSchemaLocation(new URI("datasets/data.fdml")).right.get.toString
    assertEquals("http://latis-data.io/schemas/1.0/fdml-with-text-adapter.xsd", uri)
  }

}
