package latis.output

import munit.CatsEffectSuite

import latis.dataset.Dataset
import latis.dataset.MemoizedDataset
import latis.data.Data
import latis.data.Sample
import latis.data.SampledFunction
import latis.dsl.DatasetGenerator
import latis.metadata.Metadata
import latis.model._
import latis.util.Identifier._

class CsvEncoderSuite extends CatsEffectSuite {

  val ds: Dataset = DatasetGenerator("x -> (a: int, b: double, c: string)")

  test("encode a dataset to CSV") {
    val enc = CsvEncoder()
    val expectedCsv = List(
      "0,0,0.0,a",
      "1,1,1.0,b",
      "2,2,2.0,c"
    ).map(_ + "\r\n")
    enc.encode(ds).compile.toList.assertEquals(expectedCsv)
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

  test("drop Index scalars when encoding") {
    val ds = {
      val model: DataType = Function.from(
        Index(),
        Scalar(id"value", StringValueType)
      ).fold(throw _, identity)

      val data = SampledFunction(
        List(
          Sample(List(), List(Data.StringValue("a")))
        )
      )

      new MemoizedDataset(Metadata(id"dataset"), model, data)
    }

    val expected = List("value\r\n", "a\r\n")

    CsvEncoder.withColumnName.encode(ds).compile.toList.assertEquals(expected)
  }

  test("encode a dataset containing commas") {
    val ds = {
      val model: DataType = Function.from(
        Index(),
        Scalar(id"value", StringValueType)
      ).fold(throw _, identity)

      val data = SampledFunction(
        List(
          Sample(List(), List(Data.StringValue("a,b")))
        )
      )

      new MemoizedDataset(Metadata(id"dataset"), model, data)
    }

    val expected = List("\"a,b\"\r\n")

    CsvEncoder().encode(ds).compile.toList.assertEquals(expected)
  }

  test("encode a dataset containing double quotes") {
    val ds = {
      val model: DataType = Function.from(
        Index(),
        Scalar(id"value", StringValueType)
      ).fold(throw _, identity)

      val data = SampledFunction(
        List(
          Sample(List(), List(Data.StringValue("a\"b")))
        )
      )

      new MemoizedDataset(Metadata(id"dataset"), model, data)
    }

    val expected = List("\"a\"\"b\"\r\n")

    CsvEncoder().encode(ds).compile.toList.assertEquals(expected)
  }

  test("encode a dataset containing CRLFs") {
    val ds = {
      val model: DataType = Function.from(
        Index(),
        Scalar(id"value", StringValueType)
      ).fold(throw _, identity)

      val data = SampledFunction(
        List(
          Sample(List(), List(Data.StringValue("a\r\nb")))
        )
      )

      new MemoizedDataset(Metadata(id"dataset"), model, data)
    }

    val expected = List("\"a\r\nb\"\r\n")

    CsvEncoder().encode(ds).compile.toList.assertEquals(expected)
  }
}
