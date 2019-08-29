package latis.data

import org.junit._
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite

class TestSample extends JUnitSuite {
  
  @Test
  def sample_extract(): Unit = {
    TestSample.sample match {
      case (DomainData(Real(a)), RangeData(Index(b))) =>
        assertEquals(1.0, a, 0)
        assertEquals(2, b)
    }
  }
}

/**
 * Provide reusable Samples for testing.
 */
object TestSample {
  
  val sample = (DomainData(1.0), RangeData(2))
  
  //TODO: add more test samples
}