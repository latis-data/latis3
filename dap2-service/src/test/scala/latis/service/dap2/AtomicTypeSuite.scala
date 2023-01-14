package latis.service.dap2

import java.net.URL
import java.nio.charset.StandardCharsets

import munit.CatsEffectSuite
import scodec.bits.BitVector

import latis.model._
import latis.service.dap2.AtomicType._
import latis.service.dap2.AtomicTypeValue._
import latis.util.Identifier.IdentifierStringContext

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

  def arraysEqual(obtained: Array[Byte], expected: Array[Byte]): Unit = {
    val obStr = obtained.foldLeft("")((acc, elem) => acc + f"$elem%02x\t")
    val exStr = expected.foldLeft("")((acc, elem) => acc + f"$elem%02x\t")
    assertEquals(obStr, exStr)
  }
  val byteVal: ByteValue = ByteValue(124)
  val byteArr: Array[Byte] = Array(0x7c.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte)
  val int16Val: Int16Value = Int16Value(-2140)
  val int16Arr: Array[Byte] = Array(0xf7.toByte, 0xa4.toByte, 0x00.toByte, 0x00.toByte)
  val uint16Val: UInt16Value = UInt16Value(63396)
  val uint16Arr: Array[Byte] = Array(0xf7.toByte, 0xa4.toByte, 0x00.toByte, 0x00.toByte)
  val int32Val: Int32Value = Int32Value(-140215250)
  val int32Arr: Array[Byte] = Array(0xf7.toByte, 0xa4.toByte, 0x7c.toByte, 0x2e.toByte)
  val uint32Val: UInt32Value = UInt32Value(4154752046L)
  val uint32Arr: Array[Byte] = Array(0xf7.toByte, 0xa4.toByte, 0x7c.toByte, 0x2e.toByte)
  val float32Val: Float32Value = Float32Value(3.14159f)
  val float32Arr: Array[Byte] = Array(0x40.toByte, 0x49.toByte, 0x0f.toByte, 0xd0.toByte)
  val float64Val: Float64Value = Float64Value(3.1415926535)
  val float64Arr: Array[Byte] = Array(0x40.toByte, 0x09.toByte, 0x21.toByte, 0xfb.toByte, 0x54.toByte, 0x41.toByte, 0x17.toByte, 0x44.toByte)
  val str0: StringValue = StringValue("This is a test? Strings!")
  val str0Bytes: Array[Byte] = "This is a test? Strings!".getBytes(StandardCharsets.UTF_8)
  val str3: StringValue = StringValue("1 more than a 4x?")
  val str3Bytes: Array[Byte] = "1 more than a 4x?\u0000\u0000\u0000".getBytes(StandardCharsets.UTF_8)
  val str2: StringValue = StringValue("This str needs 2: ")
  val str2Bytes: Array[Byte] = "This str needs 2: \u0000\u0000".getBytes(StandardCharsets.UTF_8)
  val str1: StringValue = StringValue("Close, but 1 chr required..")
  val str1Bytes: Array[Byte] = "Close, but 1 chr required..\u0000".getBytes(StandardCharsets.UTF_8)
  val urlValue: UrlValue = UrlValue(new URL("https://lasp.colorado.edu"))
  val urlBytes: Array[Byte] = "https://lasp.colorado.edu\u0000\u0000\u0000".getBytes(StandardCharsets.UTF_8)

  test("the codecs correctly encode/decode AtomicTypeValues") {
    arraysEqual(Byte.codec.encode(byteVal).getOrElse(???).toByteArray, byteArr)
    assertEquals(Byte.codec.decode(BitVector(byteArr)).getOrElse(???).value, byteVal)
    arraysEqual(Int16.codec.encode(int16Val).getOrElse(???).toByteArray, int16Arr)
    assertEquals(Int16.codec.decode(BitVector(int16Arr)).getOrElse(???).value, int16Val)
    arraysEqual(UInt16.codec.encode(uint16Val).getOrElse(???).toByteArray, uint16Arr)
    assertEquals(UInt16.codec.decode(BitVector(uint16Arr)).getOrElse(???).value, uint16Val)
    arraysEqual(Int32.codec.encode(int32Val).getOrElse(???).toByteArray, int32Arr)
    assertEquals(Int32.codec.decode(BitVector(int32Arr)).getOrElse(???).value, int32Val)
    arraysEqual(UInt32.codec.encode(uint32Val).getOrElse(???).toByteArray, uint32Arr)
    assertEquals(UInt32.codec.decode(BitVector(uint32Arr)).getOrElse(???).value, uint32Val)
    arraysEqual(Float32.codec.encode(float32Val).getOrElse(???).toByteArray, float32Arr)
    assertEquals(Float32.codec.decode(BitVector(float32Arr)).getOrElse(???).value, float32Val)
    arraysEqual(Float64.codec.encode(float64Val).getOrElse(???).toByteArray, float64Arr)
    assertEquals(Float64.codec.decode(BitVector(float64Arr)).getOrElse(???).value, float64Val)
    arraysEqual(String.codec.encode(str0).getOrElse(???).toByteArray, str0Bytes)
    assertEquals(String.codec.decode(BitVector(str0Bytes)).getOrElse(???).value, str0)
    arraysEqual(String.codec.encode(str3).getOrElse(???).toByteArray, str3Bytes)
    assertEquals(String.codec.decode(BitVector(str3Bytes)).getOrElse(???).value, str3)
    arraysEqual(String.codec.encode(str2).getOrElse(???).toByteArray, str2Bytes)
    assertEquals(String.codec.decode(BitVector(str2Bytes)).getOrElse(???).value, str2)
    arraysEqual(String.codec.encode(str1).getOrElse(???).toByteArray, str1Bytes)
    assertEquals(String.codec.decode(BitVector(str1Bytes)).getOrElse(???).value, str1)
    arraysEqual(Url.codec.encode(urlValue).getOrElse(???).toByteArray, urlBytes)
    assertEquals(Url.codec.decode(BitVector(urlBytes)).getOrElse(???).value, urlValue)
  }

  test("fromScalar generates correct AtomicTypes") {
    assertEquals(fromScalar(Scalar(id"double", DoubleValueType)).getOrElse(???).toString, "Float64")
    assertEquals(fromScalar(Scalar(id"float", FloatValueType)).getOrElse(???).toString, "Float32")
    assertEquals(fromScalar(Scalar(id"int", IntValueType)).getOrElse(???).toString, "Int32")
    assertEquals(fromScalar(Scalar(id"short", ShortValueType)).getOrElse(???).toString, "Int16")
    assertEquals(fromScalar(Scalar(id"byte", ByteValueType)).getOrElse(???).toString, "Byte")
    assertEquals(fromScalar(Scalar(id"str", StringValueType)).getOrElse(???).toString, "String")
    assert(fromScalar(Scalar(id"bool", BooleanValueType)).isLeft)
    assert(fromScalar(Scalar(id"chr", CharValueType)).isLeft)
    assert(fromScalar(Scalar(id"long", LongValueType)).isLeft)
    assert(fromScalar(Scalar(id"bin", BinaryValueType)).isLeft)
    assert(fromScalar(Scalar(id"bigint", BigIntValueType)).isLeft)
    assert(fromScalar(Scalar(id"bigdec", BigDecimalValueType)).isLeft)
  }
}
