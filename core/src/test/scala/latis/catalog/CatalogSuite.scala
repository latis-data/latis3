package latis.catalog

import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream
import munit.CatsEffectSuite

import latis.dataset.Dataset
import latis.dsl.DatasetGenerator
import latis.util.Identifier.*

class CatalogSuite extends CatsEffectSuite {

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
  private def listAll(c: Catalog): IO[List[Dataset]] = {
    def go(c: Catalog): Stream[IO, Dataset] = {
      c.datasets ++ c.catalogs.toList.foldMap { case (_, c) => go(c) }
    }
    go(c).compile.toList
  }

  test("list datasets in a single catalog") {
    val expected = List("ds1", "ds2")

    c1.datasets.map(_.id.get.asString).compile.toList.assertEquals(expected)
  }

  test("find datasets in a single catalog") {
    val expected = Option("ds2")

    c1.findDataset(id"ds2").map { ds =>
      assertEquals(ds.map(_.id.get.asString), expected)
    }
  }

  test("list datasets in a combined catalog") {
    val expected = List("ds1", "ds2", "ds3")

    combined.datasets.map(_.id.get.asString).compile.toList.assertEquals(expected)
  }

  test("find datasets on the left side of a combined catalog") {
    val expected = Option("ds2")

    combined.findDataset(id"ds2").map { ds =>
      assertEquals(ds.map(_.id.get.asString), expected)
    }
  }

  test("find datasets on the right side of a combined catalog") {
    val expected = Option("ds3")

    combined.findDataset(id"ds3").map { ds =>
      assertEquals(ds.map(_.id.get.asString), expected)
    }
  }

  test("find catalog in a nested catalog") {
    nested.findCatalog(id"b.c")
      .fold(fail("Failed to find catalog"))(assertEquals(_, c2))
  }

  test("find dataset in a nested catalog") {
    nested.findDataset(id"b.c.ds3").map { ds =>
      ds.map(_.id.get)
        .fold(fail("Failed to find dataset"))(assertEquals(_, id"ds3"))
    }
  }

  test("filter(_ => true) is identity") {
    listAll(nested.filter(_ => true)).map(_.length).assertEquals(3)
  }

  test("filter(_ => false) removes all datasets") {
    listAll(nested.filter(_ => false)).map(_.isEmpty).assert
  }
}
