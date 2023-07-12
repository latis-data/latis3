package latis.data

import munit.FunSuite

class LinearSetSuite extends FunSuite {

  val set1D = LinearSet1D(0.0, 1.0, 10)

  test("1D linear set should have a min") {
    assertEquals(set1D.min, DomainData(0.0))
  }

  test("1D linear set should have a max") {
    assertEquals(set1D.max, DomainData(9.0))
  }

  test("1D linear set should have a length") {
    assertEquals(set1D.length, 10)
  }

  test("1D linear set should provide an index of a set element") {
    assertEquals(set1D.indexOf(DomainData(3.0)), 3)
  }

  test("1D linear set should provide an index of -1 for values not equal to a set element") {
    assertEquals(set1D.indexOf(DomainData(3.3)), -1)
  }


  val set2D = LinearSet2D(set1D, set1D)

  test("2D linear set should have a min") {
    assertEquals(set2D.min, DomainData(0.0, 0.0))
  }

  test("2D linear set should have a max") {
    assertEquals(set2D.max, DomainData(9.0, 9.0))
  }
}
