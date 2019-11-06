package latis.output

import latis.model.Dataset
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class TextEncoderSpec extends FlatSpec {

  /**
   * Instance of TextEncoder for testing.
   */
  val enc = new TextEncoder
  val ds: Dataset = Dataset.fromName("data")
  val expectedOutput = List("0 -> (1,1.1,a)",
                            "1 -> (2,2.2,b)",
                            "2 -> (4,3.3,c)")

  "A Text encoder" should "encode a dataset to Text" in {
    val encodedList = enc.encode(ds).compile.toList.unsafeRunSync()
    encodedList should be (expectedOutput)
  }
}
