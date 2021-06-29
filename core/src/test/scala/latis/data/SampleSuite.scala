package latis.data

import java.io._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Inside.inside

class SampleSuite extends AnyFunSuite {

  val sample = (DomainData(1.0), RangeData(2))

  test("extract sample") {
    inside(sample) {
      case (DomainData(Real(a)), RangeData(IndexDatum(b))) =>
        assert(a == 1.0)
        assert(b == 2)
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
    assert(bytes.length == 639)
  }

}
