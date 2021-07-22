package latis.util

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import fs2._
import org.scalatest.funsuite.AnyFunSuite

import latis.catalog.Catalog
import latis.dataset.Dataset
import latis.dsl.DatasetGenerator
import latis.util.Identifier.IdentifierStringContext

class DatasetTesterSuite extends AnyFunSuite {

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

  ignore("test data") {
    tester.flatMap { dsTester =>
      lines.traverse(dsTester.testLine)
    }.unsafeRunSync()
  }
}
