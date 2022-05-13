package latis.output

import munit.CatsEffectSuite

import latis.dataset.Dataset
import latis.dsl.DatasetGenerator

class CsvEncoderSuite extends CatsEffectSuite {

  val ds: Dataset = DatasetGenerator("x -> (a: int, b: double, c: string)")

  test("encode a dataset to CSV") {
    val enc = CsvEncoder()
    val expectedCsv = List(
      "0,0,0.0,a",
      "1,1,1.0,b",
      "2,2,2.0,c"
    ).map(_ + "\r\n")
    enc.encode(ds).compile.toList.map { csvList =>
      assertEquals(csvList, expectedCsv)
    }
  }

  test("encode a dataset to CSV with a header") {
    val enc = CsvEncoder.withColumnName
    val expectedHeader = "x,a,b,c"

    val expectedCsv = List(
      expectedHeader,
      "0,0,0.0,a",
      "1,1,1.0,b",
      "2,2,2.0,c"
    ).map(_ + "\r\n")

    enc.encode(ds).compile.toList.map { csvList =>
      assertEquals(csvList.head, expectedHeader + "\r\n")
      assertEquals(csvList, expectedCsv)
    }

  }
}
