package latis.data

import org.junit._
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite

class TestData extends JUnitSuite {
  
  @Test
  def data_value_equality(): Unit = {
    val d1 = Data(1.1)
    val d2 = Data(1.1)
    assertEquals(d1, d2)
  }
  
  @Test
  def domain_data_equality(): Unit = {
    val dd1 = DomainData(1.1, 1.1f)
    val dd2 = DomainData(1.1, 1.1f)
    assertEquals(dd1, dd2)
  }
  
  @Test
  def double_value_does_not_equal_float_value(): Unit = {
    val d: Double = 1.0d
    val f: Float  = 1.0f
    val dd = Data(1.0f)
    val df = Data(1.0d)
    
    //Note, Doubles can equals Floats (but not always, e.g. 1.1)
    assertEquals(d, f, 0)
    assertNotEquals(dd, df)
  }
}
