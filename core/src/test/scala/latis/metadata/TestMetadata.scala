package latis.metadata

import org.junit._
import org.junit.Assert._
import org.scalatestplus.junit.JUnitSuite

class TestMetadata extends JUnitSuite {
  
  /**
   * Instance of Metadata for testing.
   * Note that Metadata extends PropertiesLike 
   * which is more thoroughly tested.
   */
  val testmd = Metadata("id" -> "testds")
  
  /**
   * Instance of MetadataLike for testing.
   */
  val mlike = new MetadataLike {
    def metadata = testmd
  }
  
  @Test
  def get_metadata_property(): Unit = {
    testmd.getProperty("id") match {
      case Some(id) => assertEquals("testds", id)
      case _ => fail
    }
  }
  
  @Test
  def get_metadata_like_property(): Unit = {
    mlike("id") match {
      case Some(id) => assertEquals("testds", id)
      case _ => fail
    }
  }
  
  @Test
  def get_metadata_like_invalid_property(): Unit = {
    val result = mlike("invalid")
    assertTrue(result.isEmpty)
  }
}