package latis.data

import TestScalarData._
import TestTupleData._
  
import org.junit._
import org.junit.Assert._

class TestTupleData {

//  @Test
//  def simple_tuple(): Unit = {
//    flatTuple match {
//      case TupleData(_, sd, ScalarData(Complex(_,i))) => 
//        assertEquals(ScalarData("3.14"), sd)
//        assertEquals(2.0, i, 0)
//    }
//  }
//  
//  @Test
//  def nested_tuple(): Unit = {
//    nestedTuple match {
//      case TupleData(TupleData(_, sd), _) =>
//        assertEquals(ScalarData("3.14"), sd)
//    }
//  }

}

object TestTupleData {
  
//  val flatTuple = TupleData(doubleScalar, stringScalar, complexScalar)
//  
//  val nestedTuple = {
//    val inner = TupleData(doubleScalar, stringScalar)
//    TupleData(inner, complexScalar)
//  }
}