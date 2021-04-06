package latis.util

import org.junit._
import org.junit.Assert._
import org.scalatestplus.junit.JUnitSuite

class TestPropertiesLike extends JUnitSuite {
  /**
   * Instance of PropertiesLike for testing.
   */
  val plike = new PropertiesLike {
    val properties = Map("foo" -> "bar")
  }

  @Test
  def get_property(): Unit =
    plike.getProperty("foo") match {
      case Some(s) => assertEquals("bar", s)
      case _       => fail
    }

  @Test
  def get_invalid_property(): Unit = {
    val result = plike.getProperty("invalid")
    assertTrue(result.isEmpty)
  }

  @Test
  def get_property_with_default(): Unit = {
    val result = plike.getProperty("foo", "baz")
    assertEquals("bar", result)
  }

  @Test
  def get_invalid_property_with_default(): Unit = {
    val result = plike.getProperty("invalid", "baz")
    assertEquals("baz", result)
  }

  @Test
  def unsafe_get_property(): Unit = {
    val result = plike.unsafeGet("foo")
    assertEquals("bar", result)
  }

  @Test(expected = classOf[NoSuchElementException])
  def unsafe_get_invalid_property(): Unit = {
    val result = plike.unsafeGet("invalid")
  }
}
