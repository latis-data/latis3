package latis.data

import munit.FunSuite

class BinSetSuite extends FunSuite {

  //Note, bin center semantics
  val set1D = BinSet1D(0.0, 1.0, 10)

  test("1D bin set should have a min") {
    assertEquals(set1D.min, DomainData(0.0))
  }

  test("1D bin set should have a max") {
    assertEquals(set1D.max, DomainData(9.0))
  }

  test("1D bin set should have a length") {
    assertEquals(set1D.length, 10)
  }

  test("1D bin set should provide an index of a bin for an edge value") {
    assertEquals(set1D.indexOf(DomainData(3.0)), 3)
  }

  test("1D bin set should provide an index of a bin") {
    assertEquals(set1D.indexOf(DomainData(3.3)), 3)
  }


  val set2D = BinSet2D(set1D, set1D)

  test("2D linear set should have a min") {
    assertEquals(set2D.min, DomainData(0.0, 0.0))
  }

  test("2D linear set should have a max") {
    assertEquals(set2D.max, DomainData(9.0, 9.0))
  }

  test("2D linear set should provide an index of a bin") {
    assertEquals(set2D.indexOf(DomainData(1.3, 2.0)), 12)
  }
}
