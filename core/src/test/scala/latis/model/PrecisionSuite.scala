package latis.model

import org.scalatest.EitherValues._
import org.scalatest.funsuite.AnyFunSuite

import latis.data.Data
import latis.metadata.Metadata

class PrecisionSuite extends AnyFunSuite {

  private val md = Metadata("id" -> "a", "type" -> "double", "precision" -> "2")
  private val scalar: Scalar = Scalar.fromMetadata(md).value

  test("format with precision") {
    assert(scalar.formatValue(Data.DoubleValue(1.234)) == "1.23")
  }

  test("format with precision rounds up") {
    assert(scalar.formatValue(Data.DoubleValue(1.235)) == "1.24")
  }

  test("format with 0 precision") {
    val md = Metadata("id" -> "a", "type" -> "double", "precision" -> "0")
    val scalar: Scalar = Scalar.fromMetadata(md).value
    assert(scalar.formatValue(Data.DoubleValue(1.235)) == "1")
  }

}
