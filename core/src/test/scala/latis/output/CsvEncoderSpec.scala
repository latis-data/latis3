package latis.output

import scala.util.Properties.lineSeparator

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.dataset.Dataset
import latis.util.Identifier.IdentifierStringContext

class CsvEncoderSpec extends AnyFlatSpec {
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
