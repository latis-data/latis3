package latis.util

import org.scalatest.funsuite.AnyFunSuite

class PropertiesLikeSuite extends AnyFunSuite {
  
  /**
   * Instance of PropertiesLike for testing.
   */
  val plike = new PropertiesLike {
    val properties = Map("foo" -> "bar")
  }
  
  test ("get property") {
    plike.getProperty("foo") match {
      case Some(s) => assert(s == "bar")
      case _ => fail
    }
  }

  test ("get invalid property") {
    val result = plike.getProperty("invalid")
    assert(result.isEmpty)
  }

  test ("get property with default") {
    val result = plike.getProperty("foo", "baz")
    assert(result == "bar")
  }

  test ("get invalid property with default") {
    val result = plike.getProperty("invalid", "baz")
    assert(result == "baz")
  }

  test ("unsafe get property") {
    val result = plike.unsafeGet("foo")
    assert(result == "bar")
  }

  test ("unsafe get invalid property") {
    assertThrows[NoSuchElementException] {plike.unsafeGet("invalid")}
  }
}
