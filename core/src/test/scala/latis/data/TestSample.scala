package latis.data

import org.junit._
import org.junit.Assert._

class TestSample {
  
  @Test
  def sample_extract(): Unit = {
    TestSample.sample match {
      case (DomainData(a: Double), RangeData(b: Int)) =>
        assertEquals(1.0, a, 0)
        assertEquals(2, b)
    }
  }
  
  @Test
  def ordering(): Unit = {
    val l = List(
      DomainData(1.2, 3, "a"),
      DomainData(1.1, 4, ""),
      DomainData(1.2, 3, "b")
    )
    val expected = List("", "a", "b")
    l.sorted(DomainOrdering) zip expected foreach {
      case (DomainData(_,_,v), x) => assertEquals(x, v)
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