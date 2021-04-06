package latis.util

import java.net.URI

import org.scalatest.EitherValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class FdmlUtilsSpec extends AnyFlatSpec {
  "FdmlUtils.getSchemaLocation" should "get the schema URI" in {
    val uri = FdmlUtils
      .getSchemaLocation(
        new URI("datasets/data.fdml")
      )
      .right
      .value
      .toString

    uri should be("http://latis-data.io/schemas/1.0/fdml-with-text-adapter.xsd")
  }

  "FdmlUtils.validateFdml" should "validate valid FDML" in {
    FdmlUtils.validateFdml("datasets/data.fdml") match {
      case Left(e) => fail(s"Valid FDML failed to validate: $e")
      case _       => succeed
    }
  }

  it should "handles missing files gracefully" in {
    val validation = FdmlUtils.validateFdml("nope.fdml")

    validation.left.value.getMessage should startWith("Failed to read URI")
  }
}
