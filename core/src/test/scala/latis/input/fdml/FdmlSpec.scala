package latis.input.fdml

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.model.DoubleValueType

class FdmlSpec extends FlatSpec {

  "An FAdapter" should "create a valid AdapterConfig" in {
    val config = SingleAdapter("class", Map("key" -> "value")).config.properties

    config should contain ("class" -> "class")
    config should contain ("key" -> "value")
  }

  "An FScalar" should "create valid scalar metadata" in {
    val metadata = FScalar(
      "id",
      DoubleValueType,
      Map("key" -> "value")
    ).metadata.properties

    metadata should contain ("id" -> "id")
    metadata should contain ("type" -> "double")
    metadata should contain ("key" -> "value")
  }
}
