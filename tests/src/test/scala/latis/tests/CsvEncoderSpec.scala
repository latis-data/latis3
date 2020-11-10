package latis.tests

import scala.util.Properties.lineSeparator

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.dataset.Dataset
import latis.output.CsvEncoder
import latis.util.Identifier.IdentifierStringContext

class CsvEncoderSpec extends FlatSpec {
  import CsvEncoderSpec._

  "A CSV encoder" should "encode a dataset to CSV" in {
    val enc     = CsvEncoder()
    val csvList = enc.encode(ds).compile.toList.unsafeRunSync()
    val expectedCsv = List(
      "0,1,1.1,a",
      "1,2,2.2,b",
      "2,4,3.3,c"
    ).map(_ + lineSeparator)
    csvList should be(expectedCsv)
  }

  it should "encode a dataset to CSV with a header" in {
    val enc            = CsvEncoder.withColumnName
    val csvList        = enc.encode(ds).compile.toList.unsafeRunSync()
    val expectedHeader = "time,b,c,d"
    csvList.head should be(expectedHeader)
  }
}

object CsvEncoderSpec {
  val ds: Dataset = Dataset.fromName(id"data")
}
