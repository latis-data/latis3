package latis.model

import munit.ScalaCheckSuite
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.Prop.*

import latis.data.*
import latis.metadata.Metadata
import latis.util.Identifier.*

class FormatSuite extends ScalaCheckSuite {

  private lazy val md = Metadata("id" -> "a", "type" -> "double", "precision" -> "2")
  private lazy val scalar: Scalar =
    Scalar.fromMetadata(md).fold(fail("failed to construct scalar", _), identity)
  private lazy val float: Scalar = Scalar(id"f", FloatValueType)

  test("Format double with precision") {
    assertEquals(scalar.formatValue(Data.DoubleValue(1.234)), "1.23")
  }

  test("Format double with precision rounds up") {
    assertEquals(scalar.formatValue(Data.DoubleValue(1.235)), "1.24")
  }

  test("Format double with 0 precision") {
    val md = Metadata("id" -> "a", "type" -> "double", "precision" -> "0")
    val scalar: Scalar =
      Scalar.fromMetadata(md).fold(fail("failed to construct scalar", _), identity)
    assertEquals(scalar.formatValue(Data.DoubleValue(1.235)), "1")
  }

  test("No precision for int") {
    val md = Metadata("id" -> "a", "type" -> "int", "precision" -> "2")
    assert(Scalar.fromMetadata(md).isLeft)
  }

  test("Preserve double NaN") {
    assertEquals(scalar.formatValue(Data.DoubleValue(Double.NaN)), "NaN")
  }

  test("Preserve float NaN") {
    assertEquals(float.formatValue(Data.FloatValue(Float.NaN)), "NaN")
  }

  test("Preserve double infinity") {
    assertEquals(scalar.formatValue(Data.DoubleValue(Double.PositiveInfinity)), "Infinity")
  }

  test("Preserve float infinity") {
    assertEquals(float.formatValue(Data.FloatValue(Float.PositiveInfinity)), "Infinity")
  }

  test("Preserve double minus infinity") {
    assertEquals(scalar.formatValue(Data.DoubleValue(Double.NegativeInfinity)), "-Infinity")
  }

  test("Preserve float minus infinity") {
    assertEquals(float.formatValue(Data.FloatValue(Float.NegativeInfinity)), "-Infinity")
  }

  test("Expand scientific notation if given precision") {
    assertEquals(scalar.formatValue(Data.DoubleValue(1e10)), "10000000000.00")
  }

  test("Preserve scientific notation") {
    assertEquals(float.formatValue(Data.FloatValue(1e10F)), "1.0E10")
  }

  test("Increase precision of a float") {
    val md = Metadata("id" -> "a", "type" -> "float", "precision" -> "2")
    val scalar = Scalar.fromMetadata(md).fold(fail("failed to construct scalar", _), identity)
    assertEquals(scalar.formatValue(Data.FloatValue(1.1F)), "1.10")
  }

  test("Decrease precision of a float") {
    val md = Metadata("id" -> "a", "type" -> "float", "precision" -> "2")
    val scalar = Scalar.fromMetadata(md).fold(fail("failed to construct scalar", _), identity)
    assertEquals(scalar.formatValue(Data.FloatValue(3.379F)), "3.38")
  }

  test("Format float example: 1.1") {
    assertEquals(float.formatValue(Data.FloatValue(1.1F)), "1.1")
  }

  test("Format negative float example: -2.7182") {
    assertEquals(float.formatValue(Data.FloatValue(-2.7182F)), "-2.7182")
  }

  test("Format float example: 3.379") {
    assertEquals(float.formatValue(Data.FloatValue(3.379F)), "3.379")
  }

  test("Format double example: 1.1") {
    val double = Scalar(id"d", DoubleValueType)
    assertEquals(double.formatValue(Data.DoubleValue(1.1)), "1.1")
  }

  test("Format NullData") {
    assertEquals(scalar.formatValue(NullData), "null")
  }

  test("Get error from format on a Tuple") {
    assertEquals(scalar.formatValue(Data.fromSeq(Seq(1, 2, 3))), "error")
  }

  property("Generated format double with precision") {
    val pGen: Gen[Int] = Gen.choose(0, 10)
    forAll(pGen, Arbitrary.arbitrary[Double]){ (p: Int, num: Double) =>
      val str = (s"%.${p}f").format(num)
      val md = Metadata("id" -> "a", "type" -> "double", "precision" -> p.toString)
      val formatted = Scalar
        .fromMetadata(md)
        .fold(fail("failed to construct scalar", _), identity)
        .formatValue(Data.DoubleValue(num))
      str == formatted
    }
  }

  property("Generated format double without precision") {
    forAll{ (num: Double) =>
      val str = num.toString
      val formatted = Scalar(id"d", DoubleValueType).formatValue(Data.DoubleValue(num))
      str == formatted
    }
  }

  property("Generated format float with precision") {
    val pGen: Gen[Int] = Gen.choose(0, 10)
    forAll(pGen, Arbitrary.arbitrary[Float]){ (p: Int, num: Float) =>
      val str = (s"%.${p}f").format(num)
      val md = Metadata("id" -> "a", "type" -> "float", "precision" -> p.toString)
      val formatted = Scalar
        .fromMetadata(md)
        .fold(fail("failed to construct scalar", _), identity)
        .formatValue(Data.FloatValue(num))
      str == formatted
    }
  }

  property("Generated format float without precision") {
    forAll{ (num: Float) =>
      val str = num.toString
      val formatted = Scalar(id"f", FloatValueType).formatValue(Data.FloatValue(num))
      str == formatted
    }
  }
}
