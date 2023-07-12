package latis.util

import munit.FunSuite

class PropertiesLikeSuite extends FunSuite {

  /**
   * Instance of PropertiesLike for testing.
   */
  val plike = new PropertiesLike {
    val properties = Map("foo" -> "bar")
  }

  test ("get property") {
    plike.getProperty("foo") match {
      case Some(s) => assertEquals(s, "bar")
      case _ => fail("")
    }
  }

  test ("get invalid property") {
    val result = plike.getProperty("invalid")
    assert(result.isEmpty)
  }

  test ("get property with default") {
    val result = plike.getProperty("foo", "baz")
    assertEquals(result, "bar")
  }

  test ("get invalid property with default") {
    val result = plike.getProperty("invalid", "baz")
    assertEquals(result, "baz")
  }

  test ("unsafe get property") {
    val result = plike.unsafeGet("foo")
    assertEquals(result, "bar")
  }

  test ("unsafe get invalid property") {
    intercept[NoSuchElementException](plike.unsafeGet("invalid"))
  }
}
