package latis.input.fdml

import munit.FunSuite

import latis.model.DoubleValueType
import latis.util.Identifier._

class FdmlSuite extends FunSuite {

  test("create a valid AdapterConfig from FAdapter") {
    val config = SingleAdapter("class", Map("key" -> "value")).config.properties

    assert(config.contains("class" -> "class"))
    assert(config.contains("key" -> "value"))
  }

  test("create valid scalar metadata from FScalar") {
    val metadata = FScalar(
      id"id",
      DoubleValueType,
      Map("key" -> "value")
    ).metadata.properties

    assert(metadata.toList.contains("id" -> "id"))
    assert(metadata.toList.contains("type" -> "double"))
    assert(metadata.toList.contains("key" -> "value"))
  }
}
