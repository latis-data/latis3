package latis.output

import scala.util.Properties.lineSeparator

import munit.CatsEffectSuite

import latis.dataset.Dataset
import latis.dsl.*
import latis.util.Identifier.*

class TextEncoderSuite extends CatsEffectSuite {

  /** Instance of TextEncoder for testing. */
  val enc = new TextEncoder

  test("encode a dataset to Text") {
    val ds: Dataset = DatasetGenerator("x -> (a: int, b: double, c: string)", id"foo")
    val expectedOutput = List(
      "foo: x -> (a, b, c)",
      "0 -> (0, 0.0, a)",
      "1 -> (1, 1.0, b)",
      "2 -> (2, 2.0, c)"
    ).map(_ + lineSeparator)
    enc.encode(ds).compile.toList.assertEquals(expectedOutput)
  }

  test("increment index variable after sample is filtered out") {
    val ds = DatasetGenerator("x -> a")
      .project("a")
      .select("a != 1")

    ds.samples.compile.toList.map { samples =>
      assertEquals(enc.encodeSample(ds.model, samples.head), "0 -> 0")
      assertEquals(enc.encodeSample(ds.model, samples(1)), "1 -> 2")
    }
  }

  test("start a new index generator for a new dataset encoding") {
    val ds = DatasetGenerator("x -> a").project("a")
    for {
      ss1 <- enc.encode(ds).compile.toList
      ss2 <- enc.encode(ds).compile.toList
    } yield {
      assertEquals(ss1, ss2)
    }
  }
}
