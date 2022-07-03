package latis.data

import java.io._

import munit.FunSuite

class SampleSuite extends FunSuite {

  val sample = (DomainData(1.0), RangeData(2))

  test("extract sample") {
    sample match {
      case (DomainData(Real(a)), RangeData(IndexDatum(b))) =>
        assertEquals(a, 1.0)
        assertEquals(b, 2)
      case _ => fail("wrong sample")
    }
  }

  test("serialized sample size with null data") {
    //val obj = sample //629
    //val obj = (List(1.0), List(2)) //646
    val obj = (List(1.0), List(NullData)) //639
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(obj)
    val bytes = bos.toByteArray
    //println(bytes.length)
    assertEquals(bytes.length, 639)
  }

}
