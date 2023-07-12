package latis.util

import java.net.URI

import munit.FunSuite

class FdmlUtilsSuite extends FunSuite {

  test("FdmlUtils.getSchemaLocation should get the schema URI") {
    val uri = FdmlUtils.getSchemaLocation(
      new URI("datasets/data.fdml")
    ).fold(fail("Failed to get schema location", _), _.toString())

    assertEquals(uri, "http://latis-data.io/schemas/1.0/fdml-with-text-adapter.xsd")
  }

  test("FdmlUtils.validateFdml should validate valid FDML") {
    FdmlUtils.validateFdml("datasets/data.fdml") match {
      case Left(e) => fail(s"Valid FDML failed to validate: $e")
      case _       => assert(true)
    }
  }

  test("FdmlUtils.validateFdml handles missing files gracefully") {
    val validation = FdmlUtils.validateFdml("nope.fdml")

    assert(validation.isLeft)
  }
}
