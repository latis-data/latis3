package latis.service.dap2

import java.net.URL

import munit.CatsEffectSuite

import latis.service.dap2.AtomicTypeValue._

class AtomicTypeValueSuite extends CatsEffectSuite {
  test("correct AtomicTypeValue dasStr representation") {
    assertEquals(ByteValue(42).dasStr, "42")
    assertEquals(Int16Value(42).dasStr, "42")
    assertEquals(UInt16Value(42).dasStr, "42")
    assertEquals(Int32Value(42).dasStr, "42")
    assertEquals(UInt32Value(42).dasStr, "42")
    assertEquals(Float32Value(13.14159265359f).dasStr, "13.1416")
    assertEquals(Float64Value(13.14159265359).dasStr, "13.1416")
    assertEquals(StringValue("test\\string\"quote\"").dasStr, "\"test\\\\string\\\"quote\\\"\"")
    assertEquals(UrlValue(new URL("https://lasp.colorado.edu")).dasStr, "\"https://lasp.colorado.edu\"")
  }

  test("UInts mapped to the correct min/max iff out-of-range") {
    assertEquals(UInt16Value(-42).value, 0)
    assertEquals(UInt16Value(712).value, 712)
    assertEquals(UInt16Value(123456).value, 65535)

    assertEquals(UInt32Value(-42L).value, 0L)
    assertEquals(UInt32Value(712L).value, 712L)
    assertEquals(UInt32Value(123456L).value, 123456L)
    assertEquals(UInt32Value(9876543210L).value, 4294967295L)
  }
}
