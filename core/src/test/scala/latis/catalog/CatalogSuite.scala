package latis.catalog

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

import latis.dataset.Dataset
import latis.dsl.DatasetGenerator
import latis.util.Identifier._

class CatalogSuite extends AnyFunSuite {

  val ds1 = DatasetGenerator("a: double -> b: double", id"ds1")
  val ds2 = DatasetGenerator("a: double -> b: double", id"ds2")
  val ds3 = DatasetGenerator("a: double -> b: double", id"ds3")

  val c1 = Catalog(ds1, ds2)
  val c2 = Catalog(ds3)
  val combined = c1 |+| c2
  val nested = Catalog.empty.withCatalogs(
    id"a" -> Catalog(ds1),
    id"b" -> Catalog(ds2).addCatalog(id"c", c2)
  )

  // Get a list of every dataset in a catalog and its subcatalogs.
  //
  // Datasets from the root catalog will come first, but the order of
  // subcatalogs is undefined.
  //
  // This is not useful in practice because we lose information about
  // which catalog a dataset came from, so there are no guarantees
  // that datasets in this list are unique.
  private def listAll(c: Catalog): List[Dataset] = {
    def go(c: Catalog): Stream[IO, Dataset] = {
      c.datasets ++ c.catalogs.toList.foldMap { case (_, c) => go(c) }
    }
    go(c).compile.toList.unsafeRunSync()
  }

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

  test("find catalog in a nested catalog") {
    nested.findCatalog(id"b.c")
      .fold(fail("Failed to find catalog"))(assertResult(c2)(_))
  }

  test("find dataset in a nested catalog") {
    nested.findDataset(id"b.c.ds3")
      .unsafeRunSync()
      .map(_.id.get)
      .fold(fail("Failed to find dataset"))(assertResult(id"ds3")(_))
  }

  test("filter(_ => true) is identity") {
    assertResult(3) {
      listAll(nested.filter(_ => true)).length
    }
  }

  test("filter(_ => false) removes all datasets") {
    assert {
      listAll(nested.filter(_ => false)).isEmpty
    }
  }
}
