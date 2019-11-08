package latis.output

import latis.model.Dataset
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import scala.util.Properties.lineSeparator

class TextEncoderSpec extends FlatSpec {

  /**
   * Instance of TextEncoder for testing.
   */
  val enc = new TextEncoder
  val ds: Dataset = Dataset.fromName("data")
  val expectedOutput: Seq[String] = List(
    "a -> (b, c, d)",
    "0 -> (1,1.1,a)",
    "1 -> (2,2.2,b)",
    "2 -> (4,3.3,c)"
  ).map(_ + lineSeparator)

  "A Text encoder" should "encode a dataset to Text" in {
    val encodedList = enc.encode(ds).compile.toList.unsafeRunSync()
    encodedList should be (expectedOutput)
  }
}
