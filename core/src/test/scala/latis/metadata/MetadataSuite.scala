package latis.metadata

import munit.FunSuite

class MetadataSuite extends FunSuite {

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
}
