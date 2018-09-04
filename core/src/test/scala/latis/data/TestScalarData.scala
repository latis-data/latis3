package latis.data

import TestScalarData._

import org.junit._
import org.junit.Assert._

class TestScalarData {
  
  import TestScalarData._
  
  @Test
  def double_as_data(): Unit = {
    double match {
      case ScalarData(v: Double) => assertEquals(1.0, v, 0)
      case _ => fail
    }
  }
  
  @Test
  def double_as_double_data(): Unit = {
    double match {
      case DoubleData(v: Double) => assertEquals(1.0, v, 0)
      //TODO: doesn't match!
      case _ => fail
    }
  }
  
//  @Test
//  def string_scalar(): Unit = {
//    string match {
//      case ScalarData(v: String) => assertEquals("3.14", v)
//      case _ => fail
//    }
//  }
}

object TestScalarData {
  
  val boolean = BooleanData.True
  val byte    = ByteData(0x01.toByte)
  val char    = CharData('A')
  val short   = ShortData(1.toShort)
  val int     = IntData(1)
  val float   = FloatData(1.0f)
  val long    = LongData(1)
  val double  = ScalarData(1.0)
  val string  = Text("one")
  val integer = Integer(BigInt(1))
  val real    = Real(BigDecimal(1.0))
  val complex = ComplexData((1.0, 1.0))

}
