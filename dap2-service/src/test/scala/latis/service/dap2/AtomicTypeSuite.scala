package latis.service.dap2

import munit.CatsEffectSuite
import java.net.URL

import latis.service.dap2.AtomicType._

class AtomicTypeSuite extends CatsEffectSuite {
  test("correct AtomicType toString representation") {
    assertEquals(Byte.toString, "Byte")
    assertEquals(Int16.toString, "Int16")
    assertEquals(UInt16.toString, "UInt16")
    assertEquals(Int32.toString, "Int32")
    assertEquals(UInt32.toString, "UInt32")
    assertEquals(Float32.toString, "Float32")
    assertEquals(Float64.toString, "Float64")
    assertEquals(String.toString, "String")
    assertEquals(Url.toString, "Url")
  }

  test("correct AtomicType asDasString representation") {
    assertEquals(Byte.asDasString(42), "42")
    assertEquals(Int16.asDasString(42), "42")
    assertEquals(UInt16.asDasString(42), "42")
    assertEquals(Int32.asDasString(42), "42")
    assertEquals(UInt32.asDasString(42), "42")
    assertEquals(Float32.asDasString(13.14159265359f), "13.1416")
    assertEquals(Float64.asDasString(13.14159265359), "13.1416")
    assertEquals(String.asDasString("test\\string\"quote\""), "\"test\\\\string\\\"quote\\\"\"")
    assertEquals(Url.asDasString(new URL("https://lasp.colorado.edu")), "\"https://lasp.colorado.edu\"")
  }

  test("asDasString correctly maps out-of-range UInts to the correct min/max") {
    assertEquals(UInt16.asDasString(-42), "0")
    assertEquals(UInt16.asDasString(123456), "65535")
    assertEquals(UInt32.asDasString(-42L), "0")
    assertEquals(UInt32.asDasString(9876543210L), "4294967295")
  }
}
