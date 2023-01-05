package latis.service.dap2

import munit.CatsEffectSuite
import java.net.URL
import java.nio.charset.StandardCharsets

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

  val url = new URL("https://lasp.colorado.edu")
  test("correct AtomicType asDasString representation") {
    assertEquals(Byte.asDasString(42), "42")
    assertEquals(Int16.asDasString(42), "42")
    assertEquals(UInt16.asDasString(42), "42")
    assertEquals(Int32.asDasString(42), "42")
    assertEquals(UInt32.asDasString(42), "42")
    assertEquals(Float32.asDasString(13.14159265359f), "13.1416")
    assertEquals(Float64.asDasString(13.14159265359), "13.1416")
    assertEquals(String.asDasString("test\\string\"quote\""), "\"test\\\\string\\\"quote\\\"\"")
    assertEquals(Url.asDasString(url), "\"https://lasp.colorado.edu\"")
  }

  test("asDasString correctly maps out-of-range UInts to the correct min/max") {
    assertEquals(UInt16.asDasString(-42), "0")
    assertEquals(UInt16.asDasString(123456), "65535")
    assertEquals(UInt32.asDasString(-42L), "0")
    assertEquals(UInt32.asDasString(9876543210L), "4294967295")
  }

  def arraysEqual(obtained: Array[Byte], expected: Array[Byte]): Unit = {
    val obStr = obtained.foldLeft("")((acc, elem) => acc + f"$elem%02x\t")
    val exStr = expected.foldLeft("")((acc, elem) => acc + f"$elem%02x\t")
    assertEquals(obStr, exStr)
  }

  test("correct AtomicType asByteArray encodings") {
    arraysEqual(Byte.asByteArray(124.byteValue), Array(0x7c.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte))
    arraysEqual(Int16.asByteArray((-2140).shortValue), Array(0xf7.toByte, 0xa4.toByte, 0x00.toByte, 0x00.toByte))
    arraysEqual(UInt16.asByteArray(63396), Array(0xf7.toByte, 0xa4.toByte, 0x00.toByte, 0x00.toByte))
    arraysEqual(Int32.asByteArray(-140215250), Array(0xf7.toByte, 0xa4.toByte, 0x7c.toByte, 0x2e.toByte))
    arraysEqual(UInt32.asByteArray(4154752046L), Array(0xf7.toByte, 0xa4.toByte, 0x7c.toByte, 0x2e.toByte))
    arraysEqual(Float32.asByteArray(3.14159f), Array(0x40.toByte, 0x49.toByte, 0x0f.toByte, 0xd0.toByte))
    arraysEqual(
      Float64.asByteArray(3.1415926535),
      Array(0x40.toByte, 0x09.toByte, 0x21.toByte, 0xfb.toByte, 0x54.toByte, 0x41.toByte, 0x17.toByte, 0x44.toByte)
    )
    arraysEqual(
      String.asByteArray("This is a test? Strings!"),
      "This is a test? Strings!".getBytes(StandardCharsets.UTF_8)
    )
    arraysEqual(
      String.asByteArray("1 more than a 4x?"),
      "1 more than a 4x?\u0000\u0000\u0000".getBytes(StandardCharsets.UTF_8)
    )
    arraysEqual(
      String.asByteArray("This str needs 2: "),
      "This str needs 2: \u0000\u0000".getBytes(StandardCharsets.UTF_8)
    )
    arraysEqual(
      String.asByteArray("Close, but 1 chr required.."),
      "Close, but 1 chr required..\u0000".getBytes(StandardCharsets.UTF_8)
    )
    arraysEqual(Url.asByteArray(url), "https://lasp.colorado.edu\u0000\u0000\u0000".getBytes(StandardCharsets.UTF_8))
  }
}
