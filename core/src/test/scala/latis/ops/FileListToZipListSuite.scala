package latis.ops

import cats.effect.IO
import cats.syntax.all._
import fs2.Stream
import fs2.io.file.Path
import fs2.text.utf8

import latis.data.Data.BinaryValue
import latis.data.Data.IntValue
import latis.data.Data.LongValue
import latis.data.Data.StringValue
import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.util.Identifier._

final class FileListToZipListSuite extends munit.CatsEffectSuite {

  // Just turn the path into bytes for easy testing.
  private val loOpFetchBytes: Path => IO[Array[Byte]] =
    path => Stream
      .emit(path.toString)
      .through(utf8.encode)
      .compile
      .to(Array)
      .pure[IO]

  private val op = new FileListToZipList(loOpFetchBytes)

  test("Reject a non-file list model") {
    val model = Function.from(
      Scalar(id"time", StringValueType),
      Scalar(id"a", IntValueType)
    ).getOrElse(???)

    assert(op.applyToData(NullData, model).isLeft)
  }

  test("Make entries for each file in file list") {
    val data = SampledFunction(List(
      Sample(List(IntValue(0)), List(StringValue("file:///a/b/c/file0.ext"))),
      Sample(List(IntValue(1)), List(StringValue("file:///a/b/c/file1.ext"))),
      Sample(List(IntValue(2)), List(StringValue("file:///a/b/c/file2.ext")))
    ))

    val expected = List(
      ("file0.ext", "/a/b/c/file0.ext".getBytes("utf-8").toList),
      ("file1.ext", "/a/b/c/file1.ext".getBytes("utf-8").toList),
      ("file2.ext", "/a/b/c/file2.ext".getBytes("utf-8").toList),
    )

    val model = Function.from(
      Scalar(id"dummy", IntValueType),
      Scalar(id"uri", StringValueType)
    ).getOrElse(???)

    op.applyToData(data, model) match {
      case Left(err) => fail(err.message)
      case Right(data) => data.samples.collect {
        case Sample(DomainData(Text(name)), RangeData(bytes: BinaryValue)) =>
          (name, bytes.value.toList)
      }.compile.toList.assertEquals(expected)
    }
  }

  test("Support file lists with sizes") {
    val data = SampledFunction(List(
      Sample(List(IntValue(0)), List(StringValue("file:///a/b/c/file0.ext"), LongValue(1))),
      Sample(List(IntValue(1)), List(StringValue("file:///a/b/c/file1.ext"), LongValue(1))),
      Sample(List(IntValue(2)), List(StringValue("file:///a/b/c/file2.ext"), LongValue(1)))
    ))

    val expected = List(
      ("file0.ext", "/a/b/c/file0.ext".getBytes("utf-8").toList),
      ("file1.ext", "/a/b/c/file1.ext".getBytes("utf-8").toList),
      ("file2.ext", "/a/b/c/file2.ext".getBytes("utf-8").toList),
    )

    val model = Function.from(
      Scalar(id"dummy", IntValueType),
      Tuple.fromElements(
        Scalar(id"uri", StringValueType),
        Scalar(id"size", LongValueType)
      ).getOrElse(???)
    ).getOrElse(???)

    op.applyToData(data, model) match {
      case Left(err) => fail(err.message)
      case Right(data) =>
        data.samples.collect {
        case Sample(DomainData(Text(name)), RangeData(bytes: BinaryValue)) =>
          (name, bytes.value.toList)
      }.compile.toList.assertEquals(expected)
    }
  }

  test("Support baseUri metadata property") {
    val data = SampledFunction(List(
      Sample(List(IntValue(0)), List(StringValue("file0.ext"))),
      Sample(List(IntValue(1)), List(StringValue("file1.ext"))),
      Sample(List(IntValue(2)), List(StringValue("file2.ext")))
    ))

    val expected = List(
      ("file0.ext", "/a/b/c/file0.ext".getBytes("utf-8").toList),
      ("file1.ext", "/a/b/c/file1.ext".getBytes("utf-8").toList),
      ("file2.ext", "/a/b/c/file2.ext".getBytes("utf-8").toList),
    )

    val model = Function.from(
      Scalar(id"dummy", IntValueType),
      Scalar.fromMetadata(
        Metadata("id" -> "uri", "type" -> "string", "baseUri" -> "file:///a/b/c/")
      ).getOrElse(???)
    ).getOrElse(???)

    op.applyToData(data, model) match {
      case Left(err) => fail(err.message)
      case Right(data) => data.samples.collect {
        case Sample(DomainData(Text(name)), RangeData(bytes: BinaryValue)) =>
          (name, bytes.value.toList)
      }.compile.toList.assertEquals(expected)
    }
  }

  test("Ensure entry names are unique") {
    val data = SampledFunction(List(
      Sample(List(IntValue(0)), List(StringValue("file:///a/b/1/file.ext"))),
      Sample(List(IntValue(1)), List(StringValue("file:///a/b/2/file.ext"))),
      Sample(List(IntValue(2)), List(StringValue("file:///a/b/3/file.ext")))
    ))

    val expected = List("file.ext", "file_001.ext", "file_002.ext")

    val model = Function.from(
      Scalar(id"dummy", IntValueType),
      Scalar(id"uri", StringValueType)
    ).getOrElse(???)

    op.applyToData(data, model) match {
      case Left(err) => fail(err.message)
      case Right(data) => data.samples.map {
        case Sample(DomainData(Text(name)), _) => name
        case s => fail(s"Unexpected sample: $s")
      }.compile.toList.assertEquals(expected)
    }
  }
}
