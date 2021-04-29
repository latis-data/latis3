package latis.output

import java.nio.file.Paths

import scala.util.Properties.lineSeparator

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.catalog.FdmlCatalog
import latis.dataset.Dataset
import latis.util.Identifier.IdentifierStringContext

class CsvEncoderSpec extends AnyFlatSpec {

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
    csvList.head should be(expectedHeader + lineSeparator)

    val expectedCsv = List(
      expectedHeader,
      "0,1,1.1,a",
      "1,2,2.2,b",
      "2,4,3.3,c"
    ).map(_ + lineSeparator)
    csvList should be (expectedCsv)
  }
}
