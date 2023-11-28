package latis.metadata

import munit.FunSuite

class MetadataSuite extends FunSuite {

  /**
   * Instance of Metadata for testing.
   * Note that Metadata extends PropertiesLike
   * which is more thoroughly tested.
   */
  val testmd: Metadata = Metadata("id" -> "testds")

  /**
   * Instance of MetadataLike for testing.
   */
  val mlike: MetadataLike = new MetadataLike {
    def metadata: Metadata = testmd
  }

  test("get metadata property") {
    testmd.getProperty("id") match {
      case Some(id) => assertEquals(id, "testds")
      case _ => fail("")
    }
  }

  test("get metadata like property") {
    mlike("id") match {
      case Some(id) => assertEquals(id, "testds")
      case _ => fail("")
    }
  }

  test("get_metadata_like_invalid_property") {
    val result = mlike("invalid")
    assert(result.isEmpty)
  }

  test("add property") {
    val md = testmd + ("foo", "bar")
    val value = md.getProperty("foo").getOrElse(fail("foo property not found"))
    assertEquals(value, "bar")
  }

  test("replace property") {
    val md = testmd + ("foo", "bar") + ("foo", "baz")
    val value = md.getProperty("foo").getOrElse(fail("foo property not found"))
    assertEquals(value, "baz")
  }

  test("combine metadata with override") {
    val md1 = Metadata("id" -> "md1") + ("foo", "bar")
    val md2 = Metadata("name" -> "md2") + ("foo", "baz")
    val md = md1 ++ md2
    assertEquals(md.properties.size, 3)
    assertEquals(md.getProperty("id").getOrElse(fail("property not found")), "md1")
    assertEquals(md.getProperty("name").getOrElse(fail("property not found")), "md2")
    assertEquals(md.getProperty("foo").getOrElse(fail("property not found")), "baz")
  }
}
