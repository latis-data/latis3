package latis.service.dap2

import java.net.URL

import munit.CatsEffectSuite

import latis.data.Data
import latis.data.Data._
import latis.service.dap2.AtomicTypeValue._

class AtomicTypeValueSuite extends CatsEffectSuite {
  test("correct AtomicTypeValue dasStr representation") {
    assertEquals(AtomicTypeValue.ByteValue(42).dasStr, "42")
    assertEquals(Int16Value(42).dasStr, "42")
    assertEquals(UInt16Value(42).dasStr, "42")
    assertEquals(Int32Value(42).dasStr, "42")
    assertEquals(UInt32Value(42).dasStr, "42")
    assertEquals(Float32Value(13.14159265359f).dasStr, "13.1416")
    assertEquals(Float64Value(13.14159265359).dasStr, "13.1416")
    assertEquals(AtomicTypeValue.StringValue("test\\string\"quote\"").dasStr, "\"test\\\\string\\\"quote\\\"\"")
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

  test("fromDatum generates correct AtomicTypeValues") {
    assertEquals(fromDatum(DoubleValue(42.0)).getOrElse(???), Float64Value(42.0))
    assertEquals(fromDatum(FloatValue(42.0F)).getOrElse(???), Float32Value(42.0F))
    assertEquals(fromDatum(IntValue(42)).getOrElse(???), Int32Value(42))
    assertEquals(fromDatum(ShortValue(42)).getOrElse(???), Int16Value(42))
    assertEquals(fromDatum(Data.ByteValue(42)).getOrElse(???), AtomicTypeValue.ByteValue(42))
    assertEquals(fromDatum(Data.StringValue("test")).getOrElse(???), AtomicTypeValue.StringValue("test"))
    assert(fromDatum(BooleanValue(true)).isLeft)
    assert(fromDatum(CharValue('A')).isLeft)
    assert(fromDatum(LongValue(42L)).isLeft)
    assert(fromDatum(BinaryValue(Array(42.toByte, 10.toByte))).isLeft)
    assert(fromDatum(BigIntValue(42)).isLeft)
    assert(fromDatum(BigDecimalValue(42.0)).isLeft)
  }
}
