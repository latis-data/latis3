package latis.output

import scala.util.Properties.lineSeparator

import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.dataset.Dataset
import latis.dsl._
import latis.util.Identifier.IdentifierStringContext

class TextEncoderSpec extends AnyFlatSpec {

  /** Instance of TextEncoder for testing. */
  val enc = new TextEncoder

  "A Text encoder" should "encode a dataset to Text" in {
    val ds: Dataset = DatasetGenerator("x -> (a: int, b: double, c: string)", id"foo")
    val expectedOutput: Seq[String] = List(
      "foo: x -> (a, b, c)",
      "0 -> (0, 0.0, a)",
      "1 -> (1, 1.0, b)",
      "2 -> (2, 2.0, c)"
    ).map(_ + lineSeparator)
    val encodedList = enc.encode(ds).compile.toList.unsafeRunSync()
    encodedList should be (expectedOutput)
  }

  it should "increment index variable after sample is filtered out" in {
    val ds = DatasetGenerator("x -> a")
      .project("a")
      .select("a != 1")

    val samples = ds.samples.compile.toList.unsafeRunSync()
    enc.encodeSample(ds.model, samples(0)) should be ("0 -> 0")
    enc.encodeSample(ds.model, samples(1)) should be ("1 -> 2")
  }

  it should "start a new index generator for a new dataset encoding" in {
    val ds = DatasetGenerator("x -> a").project("a")
    (for {
      ss1 <- enc.encode(ds).compile.toList
      ss2 <- enc.encode(ds).compile.toList
    } yield {
      ss1 should be (ss2)
    }).unsafeRunSync()
  }
}
