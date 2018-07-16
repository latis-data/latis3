package latis.data

import org.junit._
import org.junit.Assert._

class TestSample {
  
  @Test
  def sample_extract(): Unit = {
    TestSample.sample match {
      case Sample(n, Array(ScalarData(a: Double), ScalarData(b: Int))) =>
        assertEquals(1, n)
        assertEquals(1.0, a, 0)
        assertEquals(2, b)
    }
  }
}

/**
 * Provide reusable Samples for testing.
 */
object TestSample {
  
  //TODO: use TestScalarData
  //TODO: >1 "apply" methods for Sample object confuses this: ScalarData doesn't match Data
  val sample = Sample(1, Array(ScalarData(1.0), ScalarData(2)))
}