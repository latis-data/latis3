package latis.output

import scala.util.Properties.lineSeparator
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import latis.dataset.Dataset

class CsvEncoderSpec extends FlatSpec {

  /**
   * Instance of CsvEncoder for testing.
   */
  val enc = new CsvEncoder
  val ds: Dataset = Dataset.fromName("data")
  val expectedCsv = List(
    "0,1,1.1,a",
    "1,2,2.2,b",
    "2,4,3.3,c"
  ).map(_ + lineSeparator)

  "A CSV encoder" should "encode a dataset to CSV" in {
    val csvList = enc.encode(ds).compile.toList.unsafeRunSync()
    csvList should be (expectedCsv)
  }
}
