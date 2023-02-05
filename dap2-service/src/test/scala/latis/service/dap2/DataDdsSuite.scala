package latis.service.dap2

import java.net.URL
import java.nio.charset.StandardCharsets

import cats.effect.IO
import fs2.Stream
import munit.CatsEffectSuite

import latis.service.dap2.AtomicType._
import latis.service.dap2.AtomicTypeValue._
import latis.service.dap2.DataDds._
import latis.util.LatisException

class DataDdsSuite extends CatsEffectSuite {
  def streamsEqual[F](obtained: Stream[IO,F], expected: Stream[IO,F]): Unit = {
    assertEquals(obtained.compile.toList.unsafeRunSync().length, expected.compile.toList.unsafeRunSync().length)
    obtained.zip(expected).foreach(p => IO(assertEquals(p._1, p._2))).compile.toList.unsafeRunSync()
  }

  val byteVal = ByteValue(124)
  val byteArr = Array(0x7c.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte)
  val int16Val = Int16Value(-2140)
  val int16Arr = Array(0xf7.toByte, 0xa4.toByte, 0x00.toByte, 0x00.toByte)
  val uint16Val = UInt16Value(63396)
  val uint16Arr = Array(0xf7.toByte, 0xa4.toByte, 0x00.toByte, 0x00.toByte)
  val int32Val = Int32Value(-140215250)
  val int32Arr = Array(0xf7.toByte, 0xa4.toByte, 0x7c.toByte, 0x2e.toByte)
  val uint32Val = UInt32Value(4154752046L)
  val uint32Arr = Array(0xf7.toByte, 0xa4.toByte, 0x7c.toByte, 0x2e.toByte)
  val float32Val = Float32Value(3.14159f)
  val float32Arr = Array(0x40.toByte, 0x49.toByte, 0x0f.toByte, 0xd0.toByte)
  val float64Val = Float64Value(3.1415926535)
  val float64Arr = Array(0x40.toByte, 0x09.toByte, 0x21.toByte, 0xfb.toByte, 0x54.toByte, 0x41.toByte, 0x17.toByte, 0x44.toByte)
  val str0 = StringValue("This is a test? Strings!")
  val str0Bytes = "This is a test? Strings!".getBytes(StandardCharsets.UTF_8)
  val str3 = StringValue("1 more than a 4x?")
  val str3Bytes = "1 more than a 4x?\u0000\u0000\u0000".getBytes(StandardCharsets.UTF_8)
  val str2 = StringValue("This str needs 2: ")
  val str2Bytes = "This str needs 2: \u0000\u0000".getBytes(StandardCharsets.UTF_8)
  val str1 = StringValue("Close, but 1 chr required..")
  val str1Bytes = "Close, but 1 chr required..\u0000".getBytes(StandardCharsets.UTF_8)
  val url = UrlValue(new URL("https://lasp.colorado.edu"))
  val urlBytes = "https://lasp.colorado.edu\u0000\u0000\u0000".getBytes(StandardCharsets.UTF_8)
  val stream = Stream.emits(Array(byteVal, int16Val, uint16Val, int32Val, uint32Val, float32Val, float64Val, str0, str3, str2, str1, url))

  test("Correct AtomicValue error handling") {
    assertIO(AtomicValue(Byte).streamBuilder(Stream.empty[IO]).isLeft, true)
    assertIO(AtomicValue(Int16).streamBuilder(Stream.empty[IO]).isLeft, true)
    assertIO(AtomicValue(UInt16).streamBuilder(Stream.empty[IO]).isLeft, true)
    assertIO(AtomicValue(Int32).streamBuilder(Stream.empty[IO]).isLeft, true)
    assertIO(AtomicValue(UInt32).streamBuilder(Stream.empty[IO]).isLeft, true)
    assertIO(AtomicValue(Float32).streamBuilder(Stream.empty[IO]).isLeft, true)
    assertIO(AtomicValue(Float64).streamBuilder(Stream.empty[IO]).isLeft, true)
    assertIO(AtomicValue(String).streamBuilder(Stream.empty[IO]).isLeft, true)
    assertIO(AtomicValue(Url).streamBuilder(Stream.empty[IO]).isLeft, true)

    assertIO(AtomicValue(Byte).streamBuilder(Stream.emit(url)).isLeft, true)
    assertIO(AtomicValue(Int16).streamBuilder(Stream.emit(byteVal)).isLeft, true)
    assertIO(AtomicValue(UInt32).streamBuilder(Stream.emit(int32Val)).isLeft, true)
    assertIO(AtomicValue(Float64).streamBuilder(Stream.emit(float32Val)).isLeft, true)
    assertIO(AtomicValue(String).streamBuilder(Stream.emit(float64Val)).isLeft, true)
    assertIO(AtomicValue(Url).streamBuilder(Stream.emit(str0)).isLeft, true)
  }

  test("Correct AtomicValue stream builder") {
    val o1 = AtomicValue(Byte).streamBuilder(stream).getOrElse(???).unsafeRunSync()
    streamsEqual(o1._1, Stream.emits(byteArr))
    streamsEqual(o1._2, stream.tail)
    val o2 = AtomicValue(Int16).streamBuilder(o1._2).getOrElse(???).unsafeRunSync()
    streamsEqual(o2._1, Stream.emits(int16Arr))
    streamsEqual(o2._2, stream.takeRight(10))
    val o3 = AtomicValue(UInt16).streamBuilder(o2._2).getOrElse(???).unsafeRunSync()
    streamsEqual(o3._1, Stream.emits(uint16Arr))
    streamsEqual(o3._2, stream.takeRight(9))
    val o4 = AtomicValue(Int32).streamBuilder(o3._2).getOrElse(???).unsafeRunSync()
    streamsEqual(o4._1, Stream.emits(int32Arr))
    streamsEqual(o4._2, stream.takeRight(8))
    val o5 = AtomicValue(UInt32).streamBuilder(o4._2).getOrElse(???).unsafeRunSync()
    streamsEqual(o5._1, Stream.emits(uint32Arr))
    streamsEqual(o5._2, stream.takeRight(7))
    val o6 = AtomicValue(Float32).streamBuilder(o5._2).getOrElse(???).unsafeRunSync()
    streamsEqual(o6._1, Stream.emits(float32Arr))
    streamsEqual(o6._2, stream.takeRight(6))
    val o7 = AtomicValue(Float64).streamBuilder(o6._2).getOrElse(???).unsafeRunSync()
    streamsEqual(o7._1, Stream.emits(float64Arr))
    streamsEqual(o7._2, stream.takeRight(5))
    val o8 = AtomicValue(String).streamBuilder(o7._2).getOrElse(???).unsafeRunSync()
    streamsEqual(o8._1, Stream.emits(str0Bytes))
    streamsEqual(o8._2, stream.takeRight(4))
    val o9 = AtomicValue(String).streamBuilder(o8._2).getOrElse(???).unsafeRunSync()
    streamsEqual(o9._1, Stream.emits(str3Bytes))
    streamsEqual(o9._2, stream.takeRight(3))
    val o10 = AtomicValue(String).streamBuilder(o9._2).getOrElse(???).unsafeRunSync()
    streamsEqual(o10._1, Stream.emits(str2Bytes))
    streamsEqual(o10._2, stream.takeRight(2))
    val o11 = AtomicValue(String).streamBuilder(o10._2).getOrElse(???).unsafeRunSync()
    streamsEqual(o11._1, Stream.emits(str1Bytes))
    streamsEqual(o11._2, stream.takeRight(1))
    val o12 = AtomicValue(Url).streamBuilder(o11._2).getOrElse(???).unsafeRunSync()
    streamsEqual(o12._1, Stream.emits(urlBytes))
    streamsEqual(o12._2, stream.takeRight(0))
  }

  val struct = StructureConstructor(List(
    AtomicValue(Byte),
    AtomicValue(Int16),
    AtomicValue(UInt16),
    AtomicValue(Int32),
    AtomicValue(UInt32),
    StructureConstructor(List(
      AtomicValue(Float32),
      AtomicValue(Float64)
    ))
  ))

  test("Correct StructureConstructor error handling") {
    assertIO(struct.streamBuilder(stream.tail).isLeft, true)
    assertIO(struct.streamBuilder(stream.take(6)).isLeft, true)
    assertIO(struct.streamBuilder(Stream.empty).isLeft, true)
  }

  test("Correct StructureConstructor stream builder") {
    val emptyStructStream = StructureConstructor(List()).streamBuilder(stream).getOrElse(???).unsafeRunSync()
    streamsEqual(emptyStructStream._1, Stream.empty)
    streamsEqual(emptyStructStream._2, stream)
    val emptyStructEmptyStream = StructureConstructor(List()).streamBuilder(Stream.empty).getOrElse(???).unsafeRunSync()
    streamsEqual(emptyStructEmptyStream._1, Stream.empty)
    streamsEqual(emptyStructEmptyStream._2, Stream.empty)

    val o = struct.streamBuilder(stream).getOrElse(???).unsafeRunSync()
    streamsEqual(o._1, Stream.emits(
      byteArr ++ int16Arr ++ uint16Arr ++ int32Arr ++ uint32Arr ++ float32Arr ++ float64Arr
    ))
    streamsEqual(o._2, stream.takeRight(5))
  }

  test("Correct SequenceConstructor error handling") {
    assertIO(SequenceConstructor(List(struct)).streamBuilder(stream.take(7)).isLeft, true)
    assertIO(SequenceConstructor(List(struct)).streamBuilder(stream.take(7) ++ Stream.emit(END_OF_SEQUENCE)).isLeft, true)
    assertIO(SequenceConstructor(List(struct)).streamBuilder(Stream.emit(START_OF_INSTANCE) ++ stream.take(7)).isLeft, true)
    assertIO(SequenceConstructor(List(struct)).streamBuilder(Stream.emit(START_OF_INSTANCE) ++ Stream.emit(END_OF_SEQUENCE)).isLeft, true)
    assertIO(SequenceConstructor(List(struct)).streamBuilder(Stream.emit(START_OF_INSTANCE) ++ stream.take(6) ++ Stream.emit(END_OF_SEQUENCE)).isLeft, true)
    assertIO(SequenceConstructor(List(struct)).streamBuilder(Stream.emit(START_OF_INSTANCE) ++ stream.take(8) ++ Stream.emit(END_OF_SEQUENCE)).isLeft, true)
  }

  test("Correct SequenceConstructor stream builder") {
    val o1 = SequenceConstructor(List(struct)).streamBuilder(Stream.emit(END_OF_SEQUENCE)).getOrElse(???).unsafeRunSync()
    streamsEqual(o1._1, Stream.emits(END_BYTES))
    streamsEqual(o1._2, Stream.empty)
    val o2 = SequenceConstructor(List(struct)).streamBuilder(Stream.emit(END_OF_SEQUENCE) ++ stream).getOrElse(???).unsafeRunSync()
    streamsEqual(o2._1, Stream.emits(END_BYTES))
    streamsEqual(o2._2, stream)
    val b = byteArr ++ int16Arr ++ uint16Arr ++ int32Arr ++ uint32Arr ++ float32Arr ++ float64Arr
    val o3 = SequenceConstructor(List(struct)).streamBuilder(Stream.emit(START_OF_INSTANCE) ++ stream.take(7) ++ Stream.emit(END_OF_SEQUENCE)).getOrElse(???).unsafeRunSync()
    streamsEqual(o3._1, Stream.emits(START_BYTES ++ b ++ END_BYTES))
    streamsEqual(o3._2, Stream.empty)
    val o4 = SequenceConstructor(List(struct)).streamBuilder(Stream.emit(START_OF_INSTANCE) ++ stream.take(7) ++ Stream.emit(END_OF_SEQUENCE) ++ stream).getOrElse(???).unsafeRunSync()
    streamsEqual(o4._1, Stream.emits(START_BYTES ++ b ++ END_BYTES))
    streamsEqual(o4._2, stream)
    val o5 = SequenceConstructor(List(struct)).streamBuilder(Stream.emit(START_OF_INSTANCE) ++ stream.take(7) ++ stream.take(7) ++ stream.take(7) ++ Stream.emit(END_OF_SEQUENCE)).getOrElse(???).unsafeRunSync()
    streamsEqual(o5._1, Stream.emits(START_BYTES ++ b ++ b ++ b ++ END_BYTES))
    streamsEqual(o5._2, Stream.empty)
    val o6 = SequenceConstructor(List(struct)).streamBuilder(Stream.emit(START_OF_INSTANCE) ++ stream.take(7) ++ stream.take(7) ++ stream.take(7) ++ Stream.emit(END_OF_SEQUENCE) ++ stream).getOrElse(???).unsafeRunSync()
    streamsEqual(o6._1, Stream.emits(START_BYTES ++ b ++ b ++ b ++ END_BYTES))
    streamsEqual(o6._2, stream)
  }

}
