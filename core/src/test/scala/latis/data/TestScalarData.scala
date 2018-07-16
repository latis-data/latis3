package latis.data

import TestScalarData._

import org.junit._
import org.junit.Assert._

class TestScalarData {
  
  @Test
  def double_scalar(): Unit = {
    doubleScalar match {
      //case ScalarData(v: Float) => fail //won't compile
      case ScalarData(v: Double) => assertEquals(3.14, v, 0)
      case _ => fail
    }
  }
  
  @Test
  def string_scalar(): Unit = {
    stringScalar match {
      case ScalarData(v: String) => assertEquals("3.14", v)
      case _ => fail
    }
  }
  
  @Test
  def complex_scalar(): Unit = {
    complexScalar match {
      case ScalarData(v: Complex) => assertEquals(Complex(1.0, 2.0), v)
      case _ => fail
    }
  }
}

object TestScalarData {
  
  val doubleScalar: ScalarData[Double] = ScalarData(3.14)
  val stringScalar: ScalarData[String] = ScalarData("3.14")
  
  case class Complex(re: Double, im: Double)
  val complexScalar: ScalarData[Complex] = ScalarData(Complex(1.0, 2.0))
}