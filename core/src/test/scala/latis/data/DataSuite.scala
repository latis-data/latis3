package latis.data

import munit.FunSuite

class DataSuite extends FunSuite {

  test("identical Data should be equivalent") {
    val d1: Data = 1.1
    val d2: Data = 1.1

    assertEquals(d1, d2)
  }

  test("identical DomainData should be equivalent") {
    val dd1 = DomainData(1.1, 1.1f)
    val dd2 = DomainData(1.1, 1.1f)

    assertEquals(dd1, dd2)
  }

  test("Double Data values should not equal Float Data values") {
    val dd: Data = 1.0f
    val df: Data = 1.0d

    assertNotEquals(dd, df)
  }

  test("BooleanValues should be represented properly as Strings") {
    assertEquals(Data.BooleanValue(true).asString, "true")
    assertEquals(Data.BooleanValue(false).asString, "false")
  }
}
