package latis.model

import org.scalatest.EitherValues._
import org.scalatest.funsuite.AnyFunSuite

import latis.data._
import latis.metadata.Metadata
import latis.util.Identifier.IdentifierStringContext

class FormatSuite extends AnyFunSuite {

  private lazy val md = Metadata("id" -> "a", "type" -> "double", "precision" -> "2")
  private lazy val scalar: Scalar = Scalar.fromMetadata(md).value
  private lazy val float: Scalar = Scalar(id"f", FloatValueType)

  test("Format double with precision") {
    assert(scalar.formatValue(Data.DoubleValue(1.234)) == "1.23")
  }

  test("Format double with precision rounds up") {
    assert(scalar.formatValue(Data.DoubleValue(1.235)) == "1.24")
  }

  test("Format double with 0 precision") {
    val md = Metadata("id" -> "a", "type" -> "double", "precision" -> "0")
    val scalar: Scalar = Scalar.fromMetadata(md).value
    assert(scalar.formatValue(Data.DoubleValue(1.235)) == "1")
  }

  test("No precision for int") {
    val md = Metadata("id" -> "a", "type" -> "int", "precision" -> "2")
    assert(Scalar.fromMetadata(md).isLeft)
  }

  test("Preserve double NaN") {
    assert(scalar.formatValue(Data.DoubleValue(Double.NaN)) == "NaN")
  }

  test("Preserve float NaN") {
    assert(float.formatValue(Data.FloatValue(Float.NaN)) == "NaN")
  }

  test("Preserve double infinity") {
    assert(scalar.formatValue(Data.DoubleValue(Double.PositiveInfinity)) == "Infinity")
  }

  test("Preserve float infinity") {
    assert(float.formatValue(Data.FloatValue(Float.PositiveInfinity)) == "Infinity")
  }

  test("Preserve double minus infinity") {
    assert(scalar.formatValue(Data.DoubleValue(Double.NegativeInfinity)) == "-Infinity")
  }

  test("Preserve float minus infinity") {
    assert(float.formatValue(Data.FloatValue(Float.NegativeInfinity)) == "-Infinity")
  }

  test("Expand scientific notation if given precision") {
    assert(scalar.formatValue(Data.DoubleValue(1e10)) == "10000000000.00")
  }

  test("Preserve scientific notation") {
    assert(float.formatValue(Data.FloatValue(1e10F)) == "1.0E10")
  }

  test("Increase precision of a float") {
    val md = Metadata("id" -> "a", "type" -> "float", "precision" -> "2")
    val scalar = Scalar.fromMetadata(md).value
    assert(scalar.formatValue(Data.FloatValue(1.1F)) == "1.10")
  }

  test("Decrease precision of a float") {
    val md = Metadata("id" -> "a", "type" -> "float", "precision" -> "2")
    val scalar = Scalar.fromMetadata(md).value
    assert(scalar.formatValue(Data.FloatValue(3.379F)) == "3.38")
  }

  test("Format float example: 1.1") {
    assert(float.formatValue(Data.FloatValue(1.1F)) == "1.1")
  }

  test("Format negative float example: -2.7182") {
    assert(float.formatValue(Data.FloatValue(-2.7182F)) == "-2.7182")
  }

  test("Format float example: 3.379") {
    assert(float.formatValue(Data.FloatValue(3.379F)) == "3.379")
  }

  test("Format double example: 1.1") {
    val double = Scalar(id"d", DoubleValueType)
    assert(double.formatValue(Data.DoubleValue(1.1)) == "1.1")
  }

  test("Format NullData") {
    assert(scalar.formatValue(NullData) == "null")
  }

  test("Get error from format on a Tuple"){
    assert(scalar.formatValue(Data.fromSeq(Seq(1, 2, 3))) == "error")
  }

}
