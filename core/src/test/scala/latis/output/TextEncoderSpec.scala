package latis.output

import java.nio.file.Paths

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import scala.util.Properties.lineSeparator

import latis.catalog.FdmlCatalog
import latis.dataset.Dataset
import latis.util.Identifier.IdentifierStringContext

class TextEncoderSpec extends AnyFlatSpec {
  /**
   * Instance of TextEncoder for testing.
   */
  val enc = new TextEncoder
  val ds: Dataset = {
    val catalog = FdmlCatalog.fromClasspath(
      getClass().getClassLoader(),
      Paths.get("datasets"),
      validate = false
    )

    catalog.findDataset(id"data").unsafeRunSync().getOrElse {
      fail("Unable to find dataset")
    }
  }
  val expectedOutput: Seq[String] = List(
    "time -> (b, c, d)",
    "0 -> (1, 1.1, a)",
    "1 -> (2, 2.2, b)",
    "2 -> (4, 3.3, c)"
  ).map(_ + lineSeparator)

  "A Text encoder" should "encode a dataset to Text" in {
    val encodedList = enc.encode(ds).compile.toList.unsafeRunSync()
    encodedList should be(expectedOutput)
  }
}
