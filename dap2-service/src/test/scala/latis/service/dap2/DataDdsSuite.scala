package latis.service.dap2

import java.net.URL
import java.nio.charset.StandardCharsets

import munit.CatsEffectSuite
import scodec.bits.BitVector

import latis.service.dap2.AtomicType._
import latis.service.dap2.DataDds.AtomicValue

class DataDdsSuite extends CatsEffectSuite {
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
  val url = new URL("https://lasp.colorado.edu")
  val urlBytes: Array[Byte] = "https://lasp.colorado.edu\u0000\u0000\u0000".getBytes(StandardCharsets.UTF_8)
/*
  val byteAV: AtomicValue[Byte] = AtomicValue(Byte)
  val int16AV: AtomicValue[Short] = AtomicValue(Int16)
  val uint16AV: AtomicValue[Int] = AtomicValue(UInt16)
  val int32AV: AtomicValue[Int] = AtomicValue(Int32)
  val uint32AV: AtomicValue[Long] = AtomicValue(UInt32)
  val float32AV: AtomicValue[Float] = AtomicValue(Float32)
  val float64AV: AtomicValue[Double] = AtomicValue(Float64)
  val strAV: AtomicValue[String] = AtomicValue(String)
  val urlAV: AtomicValue[URL] = AtomicValue(Url)
  test("Correct AtomicValue codecs") {
    arraysEqual(byteAV.codec.encode(byteVal).require.toByteArray, byteArr)
    assertEquals(byteAV.codec.decodeValue(BitVector(byteArr)).require, byteVal)
    arraysEqual(int16AV.codec.encode(int16Val).require.toByteArray, int16Arr)
    assertEquals(int16AV.codec.decodeValue(BitVector(int16Arr)).require, int16Val)
    arraysEqual(uint16AV.codec.encode(uint16Val).require.toByteArray, uint16Arr)
    assertEquals(uint16AV.codec.decodeValue(BitVector(uint16Arr)).require, uint16Val)
    arraysEqual(int32AV.codec.encode(int32Val).require.toByteArray, int32Arr)
    assertEquals(int32AV.codec.decodeValue(BitVector(int32Arr)).require, int32Val)
    arraysEqual(uint32AV.codec.encode(uint32Val).require.toByteArray, uint32Arr)
    assertEquals(uint32AV.codec.decodeValue(BitVector(uint32Arr)).require, uint32Val)
    arraysEqual(float32AV.codec.encode(float32Val).require.toByteArray, float32Arr)
    assertEquals(float32AV.codec.decodeValue(BitVector(float32Arr)).require, float32Val)
    arraysEqual(float64AV.codec.encode(float64Val).require.toByteArray, float64Arr)
    assertEquals(float64AV.codec.decodeValue(BitVector(float64Arr)).require, float64Val)
    arraysEqual(strAV.codec.encode(str0).require.toByteArray, str0Bytes)
    assertEquals(strAV.codec.decodeValue(BitVector(str0Bytes)).require, str0)
    arraysEqual(strAV.codec.encode(str3).require.toByteArray, str3Bytes)
    assertEquals(strAV.codec.decodeValue(BitVector(str3Bytes)).require, str3)
    arraysEqual(strAV.codec.encode(str2).require.toByteArray, str2Bytes)
    assertEquals(strAV.codec.decodeValue(BitVector(str2Bytes)).require, str2)
    arraysEqual(strAV.codec.encode(str1).require.toByteArray, str1Bytes)
    assertEquals(strAV.codec.decodeValue(BitVector(str1Bytes)).require, str1)
    arraysEqual(urlAV.codec.encode(url).require.toByteArray, urlBytes)
    assertEquals(urlAV.codec.decodeValue(BitVector(urlBytes)).require, url)
  }*/
}
