package latis.output

import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.dataset.Dataset
import latis.dsl.DatasetGenerator

class CsvEncoderSpec extends AnyFlatSpec {

  val ds: Dataset = DatasetGenerator("x -> (a: int, b: double, c: string)")

  "A CSV encoder" should "encode a dataset to CSV" in {
    val enc     = CsvEncoder()
    val csvList = enc.encode(ds).compile.toList.unsafeRunSync()
    val expectedCsv = List(
      "0,0,0.0,a",
      "1,1,1.0,b",
      "2,2,2.0,c"
    ).map(_ + "\n")
    csvList should be(expectedCsv)
  }

  it should "encode a dataset to CSV with a header" in {
    val enc            = CsvEncoder.withColumnName
    val csvList        = enc.encode(ds).compile.toList.unsafeRunSync()
    val expectedHeader = "x,a,b,c"
    csvList.head should be(expectedHeader + "\n")

    val expectedCsv = List(
      expectedHeader,
      "0,0,0.0,a",
      "1,1,1.0,b",
      "2,2,2.0,c"
    ).map(_ + "\n")
    csvList should be (expectedCsv)
  }
}
