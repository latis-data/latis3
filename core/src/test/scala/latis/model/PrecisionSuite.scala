package latis.model

import org.scalatest.EitherValues._
import org.scalatest.funsuite.AnyFunSuite

import latis.data.Data
import latis.metadata.Metadata

class PrecisionSuite extends AnyFunSuite {

  private lazy val md = Metadata("id" -> "a", "type" -> "double", "precision" -> "2")
  private lazy val scalar: Scalar = Scalar.fromMetadata(md).value

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

  test("no precision for int") {
    val md = Metadata("id" -> "a", "type" -> "int", "precision" -> "2")
    assert(Scalar.fromMetadata(md).isLeft)
  }

  test("preserve a NaN") {
    assert(scalar.formatValue(Data.DoubleValue(Double.NaN)) == "NaN")
  }

  test("expand scientific notation") {
    assert(scalar.formatValue(Data.DoubleValue(1e10)) == "10000000000.00")
  }
}
