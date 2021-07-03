package latis.model2

import org.scalatest.funsuite.AnyFunSuite

import latis.data.Data
import latis.metadata.Metadata

class PrecisionSuite extends AnyFunSuite {

  val md = Metadata("id" -> "a", "type" -> "double", "precision" -> "2")
  val scalar: Scalar = Scalar.fromMetadata(md).toTry.get

  test("format with precision") {
    assert(scalar.formatValue(Data.DoubleValue(1.234)) == "1.23")
  }

  test("format with precision rounds up") {
    assert(scalar.formatValue(Data.DoubleValue(1.235)) == "1.24")
  }

  test("format with 0 precision") {
    val md = Metadata("id" -> "a", "type" -> "double", "precision" -> "0")
    val scalar: Scalar = Scalar.fromMetadata(md).toTry.get
    assert(scalar.formatValue(Data.DoubleValue(1.235)) == "1")
  }

}
