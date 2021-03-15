package latis.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.metadata.Metadata
import latis.time.Time

class ScalarSpec extends AnyFlatSpec {

  "The smart constructor" should "construct a subclass" in {
    val md = Metadata(
      "id" -> "a",
      "type" -> "string",
      "units" -> "yyyy-MM-dd",
      "class" -> "latis.time.Time"
    )
    Scalar(md) shouldBe a [Time]
  }
}
