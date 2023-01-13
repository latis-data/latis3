package latis.service.dap2

import munit.CatsEffectSuite
import java.net.URL
import java.nio.charset.StandardCharsets

import scodec.bits.BitVector

import latis.data.Data._
import latis.model._
import latis.service.dap2.AtomicType._
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
  val byteVal: Byte = 124.byteValue
  val byteArr: Array[Byte] = Array(0x7c.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte)
  val int16Val: Short = (-2140).shortValue
  val int16Arr: Array[Byte] = Array(0xf7.toByte, 0xa4.toByte, 0x00.toByte, 0x00.toByte)
  val uint16Val: Int = 63396
  val uint16Arr: Array[Byte] = Array(0xf7.toByte, 0xa4.toByte, 0x00.toByte, 0x00.toByte)
  val int32Val: Int = -140215250
  val int32Arr: Array[Byte] = Array(0xf7.toByte, 0xa4.toByte, 0x7c.toByte, 0x2e.toByte)
  val uint32Val: Long = 4154752046L
  val uint32Arr: Array[Byte] = Array(0xf7.toByte, 0xa4.toByte, 0x7c.toByte, 0x2e.toByte)
  val float32Val: Float = 3.14159f
  val float32Arr: Array[Byte] = Array(0x40.toByte, 0x49.toByte, 0x0f.toByte, 0xd0.toByte)
  val float64Val: Double = 3.1415926535
  val float64Arr: Array[Byte] = Array(0x40.toByte, 0x09.toByte, 0x21.toByte, 0xfb.toByte, 0x54.toByte, 0x41.toByte, 0x17.toByte, 0x44.toByte)
  val str0: String = "This is a test? Strings!"
  val str0Bytes: Array[Byte] = "This is a test? Strings!".getBytes(StandardCharsets.UTF_8)
  val str3: String = "1 more than a 4x?"
  val str3Bytes: Array[Byte] = "1 more than a 4x?\u0000\u0000\u0000".getBytes(StandardCharsets.UTF_8)
  val str2: String = "This str needs 2: "
  val str2Bytes: Array[Byte] = "This str needs 2: \u0000\u0000".getBytes(StandardCharsets.UTF_8)
  val str1: String = "Close, but 1 chr required.."
  val str1Bytes: Array[Byte] = "Close, but 1 chr required..\u0000".getBytes(StandardCharsets.UTF_8)
  val urlBytes: Array[Byte] = "https://lasp.colorado.edu\u0000\u0000\u0000".getBytes(StandardCharsets.UTF_8)

  test("the codec + vcodec correctly encode/decode AtomicTypes' values") {
    arraysEqual(Byte.encode(byteVal).getOrElse(???), byteArr)
    assertEquals(Byte.decode(byteArr).getOrElse(???), byteVal)
    arraysEqual(Int16.encode(int16Val).getOrElse(???), int16Arr)
    assertEquals(Int16.decode(int16Arr).getOrElse(???), int16Val)
    arraysEqual(UInt16.encode(uint16Val).getOrElse(???), uint16Arr)
    assertEquals(UInt16.decode(uint16Arr).getOrElse(???), uint16Val)
    arraysEqual(Int32.encode(int32Val).getOrElse(???), int32Arr)
    assertEquals(Int32.decode(int32Arr).getOrElse(???), int32Val)
    arraysEqual(UInt32.encode(uint32Val).getOrElse(???), uint32Arr)
    assertEquals(UInt32.decode(uint32Arr).getOrElse(???), uint32Val)
    arraysEqual(Float32.encode(float32Val).getOrElse(???), float32Arr)
    assertEquals(Float32.decode(float32Arr).getOrElse(???), float32Val)
    arraysEqual(Float64.encode(float64Val).getOrElse(???), float64Arr)
    assertEquals(Float64.decode(float64Arr).getOrElse(???), float64Val)
    arraysEqual(String.encode(str0).getOrElse(???), str0Bytes)
    assertEquals(String.decode(str0Bytes).getOrElse(???), str0)
    arraysEqual(String.encode(str3).getOrElse(???), str3Bytes)
    assertEquals(String.decode(str3Bytes).getOrElse(???), str3)
    arraysEqual(String.encode(str2).getOrElse(???), str2Bytes)
    assertEquals(String.decode(str2Bytes).getOrElse(???), str2)
    arraysEqual(String.encode(str1).getOrElse(???), str1Bytes)
    assertEquals(String.decode(str1Bytes).getOrElse(???), str1)
    arraysEqual(Url.encode(url).getOrElse(???), urlBytes)
    assertEquals(Url.decode(urlBytes).getOrElse(???), url)
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

  test("correct dataCodec generation from Scalar") {
    val dc1 = dataCodec(Scalar(id"double", DoubleValueType))
    arraysEqual(dc1.encode(DoubleValue(float64Val)).require.toByteArray, float64Arr)
    assertEquals(dc1.decode(BitVector(float64Arr)).require.value, DoubleValue(float64Val))
    val dc2 = dataCodec(Scalar(id"float", FloatValueType))
    arraysEqual(dc2.encode(FloatValue(float32Val)).require.toByteArray, float32Arr)
    assertEquals(dc2.decode(BitVector(float32Arr)).require.value, FloatValue(float32Val))
    val dc3 = dataCodec(Scalar(id"int", IntValueType))
    arraysEqual(dc3.encode(IntValue(int32Val)).require.toByteArray, int32Arr)
    assertEquals(dc3.decode(BitVector(int32Arr)).require.value, IntValue(int32Val))
    val dc4 = dataCodec(Scalar(id"short", ShortValueType))
    arraysEqual(dc4.encode(ShortValue(int16Val)).require.toByteArray, int16Arr)
    assertEquals(dc4.decode(BitVector(int16Arr)).require.value, ShortValue(int16Val))
    val dc5 = dataCodec(Scalar(id"byte", ByteValueType))
    arraysEqual(dc5.encode(ByteValue(byteVal)).require.toByteArray, byteArr)
    assertEquals(dc5.decode(BitVector(byteArr)).require.value, ByteValue(byteVal))
    val dc6 = dataCodec(Scalar(id"str0", StringValueType))
    arraysEqual(dc6.encode(StringValue(str0)).require.toByteArray, str0Bytes)
    assertEquals(dc6.decode(BitVector(str0Bytes)).require.value, StringValue(str0))
    arraysEqual(dc6.encode(StringValue(str3)).require.toByteArray, str3Bytes)
    assertEquals(dc6.decode(BitVector(str3Bytes)).require.value, StringValue(str3))
    arraysEqual(dc6.encode(StringValue(str2)).require.toByteArray, str2Bytes)
    assertEquals(dc6.decode(BitVector(str2Bytes)).require.value, StringValue(str2))
    arraysEqual(dc6.encode(StringValue(str1)).require.toByteArray, str1Bytes)
    assertEquals(dc6.decode(BitVector(str1Bytes)).require.value, StringValue(str1))
    assert(dataCodec(Scalar(id"bool", BooleanValueType)).encode(BooleanValue(true)).isFailure)
    assert(dataCodec(Scalar(id"chr", CharValueType)).encode(CharValue('A')).isFailure)
    assert(dataCodec(Scalar(id"long", LongValueType)).encode(LongValue(42L)).isFailure)
    assert(dataCodec(Scalar(id"bin", BinaryValueType)).encode(BinaryValue(Array(0x42.toByte))).isFailure)
    assert(dataCodec(Scalar(id"bigint", BigIntValueType)).encode(BigIntValue(BigInt(42))).isFailure)
    assert(dataCodec(Scalar(id"bigdec", BigDecimalValueType)).encode(BigDecimalValue(BigDecimal(3.14))).isFailure)
  }
}
