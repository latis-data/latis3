package latis.catalog

import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import org.scalatest.funsuite.AnyFunSuite

import latis.dsl.DatasetGenerator
import latis.util.Identifier._

class CatalogSuite extends AnyFunSuite {

  val ds1 = DatasetGenerator("a: double -> b: double", id"ds1")
  val ds2 = DatasetGenerator("a: double -> b: double", id"ds2")
  val ds3 = DatasetGenerator("a: double -> b: double", id"ds3")

  val c1 = Catalog(ds1, ds2)
  val c2 = Catalog(ds3)
  val combined = c1 |+| c2

  test("list datasets in a single catalog") {
    val expected = List("ds1", "ds2")

    assertResult(expected) {
      c1.datasets.map(_.id.get.asString).compile.toList.unsafeRunSync()
    }
  }

  test("find datasets in a single catalog") {
    val expected = Option("ds2")

    assertResult(expected) {
      c1.findDataset(id"ds2").unsafeRunSync().map(_.id.get.asString)
    }
  }

  test("list datasets in a combined catalog") {
    val expected = List("ds1", "ds2", "ds3")

    assertResult(expected) {
      combined.datasets.map(_.id.get.asString).compile.toList.unsafeRunSync()
    }
  }

  test("find datasets on the left side of a combined catalog") {
    val expected = Option("ds2")

    assertResult(expected) {
      combined.findDataset(id"ds2").unsafeRunSync().map(_.id.get.asString)
    }
  }

  test("find datasets on the right side of a combined catalog") {
    val expected = Option("ds3")

    assertResult(expected) {
      combined.findDataset(id"ds3").unsafeRunSync().map(_.id.get.asString)
    }
  }
}
