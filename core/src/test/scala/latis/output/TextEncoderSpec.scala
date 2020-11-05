package latis.output

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import scala.util.Properties.lineSeparator

import latis.dataset.Dataset
import latis.util.Identifier.IdentifierStringContext

class TextEncoderSpec extends FlatSpec {

  /**
   * Instance of TextEncoder for testing.
   */
  val enc = new TextEncoder
  val ds: Dataset = Dataset.fromName(id"data")
  val expectedOutput: Seq[String] = List(
    "time -> (b, c, d)",
    "0 -> (1, 1.1, a)",
    "1 -> (2, 2.2, b)",
    "2 -> (4, 3.3, c)"
  ).map(_ + lineSeparator)

  "A Text encoder" should "encode a dataset to Text" in {
    val encodedList = enc.encode(ds).compile.toList.unsafeRunSync()
    encodedList should be (expectedOutput)
  }
}
