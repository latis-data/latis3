package latis.modelORIG

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.data.Data._
import latis.metadata.Metadata

/**
 * Test the application of the "precision" metadata property
 * which rounds the string representation of real data to
 * the given number of decimal places.
 */
class PrecisionSpec extends AnyFlatSpec {
  //TODO: enforce and test that only double, float, and BigDecimal
  //      types can have a precision property.

  val real = Scalar(Metadata(
    "id" -> "a",
    "type" -> "double",
    "precision" -> "2"
  ))

  "A real scalar with precision" should "be rounded" in {
    val d = DoubleValue(1.2345)
    real.formatValue(d) should be ("1.23")
  }

  it should "round up" in {
    val d = DoubleValue(1.235)
    real.formatValue(d) should be ("1.24")
  }

  it should "not add precision to a non-real" in {
    // Not that you should be formatting an IntValue with a Scalar of type double,
    // but you could.
    val d = IntValue(1)
    real.formatValue(d) should be ("1")
  }

  it should "preserve a NaN" in {
    val d = DoubleValue(Double.NaN)
    real.formatValue(d) should be ("NaN")
  }

  it should "expand scientific notation" in {
    val d = DoubleValue(1e10)
    real.formatValue(d) should be ("10000000000.00")
  }
}
