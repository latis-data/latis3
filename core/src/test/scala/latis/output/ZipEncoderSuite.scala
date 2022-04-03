package latis.output

import java.nio.charset.StandardCharsets.UTF_8
import java.util.zip.ZipInputStream

import cats.effect.IO
import cats.effect.Resource
import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.data.SampledFunction
import latis.dataset.Dataset
import latis.dataset.MemoizedDataset
import latis.dsl.DatasetGenerator
import latis.dsl.ModelParser
import latis.metadata.Metadata
import latis.util.Identifier._
import latis.util.LatisException

final class ZipEncoderSuite extends AnyFunSuite {

  private val empty: Dataset = {
    val metadata = Metadata(id"empty")

    val model = ModelParser.unsafeParse("name: string -> bytes: binary")

    val data = SampledFunction(List.empty)

    new MemoizedDataset(metadata, model, data)
  }

  private val nonEmpty: Dataset = {
    val metadata = Metadata(id"nonEmpty")

    val model = ModelParser.unsafeParse("name: string -> bytes: binary")

    val data = {
      val samples = List(
        Sample(DomainData("first"), RangeData("1st".getBytes(UTF_8))),
        Sample(DomainData("second"), RangeData("2nd".getBytes(UTF_8))),
        Sample(DomainData("third"), RangeData("3rd".getBytes(UTF_8)))
      )

      SampledFunction(samples)
    }

    new MemoizedDataset(metadata, model, data)
  }

  private val invalid: Dataset =
    DatasetGenerator("a: int -> b: int", id"invalid")

  test("encode an empty dataset") {
    val length = (new ZipEncoder)
      .encode(empty)
      .compile
      .count
      .unsafeRunSync()

    // An empty ZIP file is 22 bytes long.
    assertResult(22)(length)
  }

  test("encode a non-empty dataset") {
    val encoded = (new ZipEncoder).encode(nonEmpty)

    fs2.io.toInputStreamResource(encoded).flatMap { is =>
      Resource.fromAutoCloseable(IO(new ZipInputStream(is)))
    }.use { zis =>
      testEntry(zis, "first", "1st".getBytes(UTF_8)) >>
      testEntry(zis, "second", "2nd".getBytes(UTF_8)) >>
      testEntry(zis, "third", "3rd".getBytes(UTF_8))
    }.unsafeRunSync()
  }

  test("fail to encode invalid dataset") {
    assertThrows[LatisException] {
      (new ZipEncoder).encode(invalid).compile.drain.unsafeRunSync()
    }
  }

  private def testEntry(
    zis: ZipInputStream,
    name: String,
    bytes: Array[Byte]
  ): IO[Unit] = {
    IO.blocking(zis.getNextEntry()).map { entry =>
      assertResult(name)(entry.getName())
    } >>
    IO.blocking {
      val read = Array.ofDim[Byte](bytes.length)
      zis.read(read, 0, bytes.length)
      read
    }.map { read =>
      assertResult(bytes)(read)
    }.void
  }
}