package latis.util

import cats.effect.IO
import cats.syntax.all.*
import fs2.*
import munit.CatsEffectSuite

import latis.catalog.Catalog
import latis.dataset.Dataset
import latis.dsl.DatasetGenerator
import latis.util.Identifier.*

class DatasetTesterSuite extends CatsEffectSuite {

  private lazy val catalog: IO[Catalog] = IO( new Catalog {
    def datasets: Stream[IO, Dataset] = Stream.emits(List(
      DatasetGenerator("x -> a", id"datasetA"),
      DatasetGenerator("(x, y) -> b", id"datasetB")
    ))
  })

  private lazy val tester = catalog.map(new DatasetTester(_))

  private lazy val lines: List[String] = List(
    "datasetA, 0, 0",
    "datasetB, 0, 0, 0"
  )

  test("test data".ignore) {
    tester.flatMap { dsTester =>
      lines.traverse(dsTester.testLine)
    }
  }
}
