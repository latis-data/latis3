package latis.input

import java.io.File

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.io.file.Files
import fs2.io.file.Flags
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.dataset.AdaptedDataset
import latis.dsl.DatasetGenerator
import latis.dsl.ModelParser
import latis.metadata.Metadata
import latis.model._
import latis.util.Identifier.IdentifierStringContext

class BinaryAdapterSpec extends AnyFlatSpec {

  "A BinaryAdapter" should "read binary data" in {
    val genDataset = DatasetGenerator("a: int -> (b: int, c: double)")
    val encodedBytes = latis.output.BinaryEncoder().encode(genDataset)
    val tempFile = Files[IO].tempFile(None, "binData", ".bin", None)
    val result = tempFile.use{ path =>
      for {
        _ <- encodedBytes.through(Files[IO].writeAll(path, Flags.Write)).compile.drain
        ds = {
          val uri = new File(path.toString).toURI
          val metadata = Metadata(id"binData")
          val model: DataType = ModelParser.unsafeParse("a: int -> (b: int, c: double)")
          val adapter = new BinaryAdapter(model)
          new AdaptedDataset(metadata, model, adapter, uri)
        }
        result <- ds.samples.compile.toList
      } yield result
    }.unsafeRunSync()

    val expected = List (
      Sample (DomainData (0), RangeData (0, 0.0) ),
      Sample (DomainData (1), RangeData (1, 1.0) ),
      Sample (DomainData (2), RangeData (2, 2.0) ),
    )
    result should be (expected)
  }
}
