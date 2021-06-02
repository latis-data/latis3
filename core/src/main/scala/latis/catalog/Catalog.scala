package latis.catalog

import cats.Monoid
import cats.data.OptionT
import cats.effect.IO
import cats.syntax.all._
import fs2.Stream

import latis.dataset.Dataset
import latis.util.Identifier

/**
 * A collection of datasets that is enumerable and can find datasets
 * by their identifier.
 */
trait Catalog {

  /** Returns all datasets in this catalog. */
  def datasets: Stream[IO, Dataset]

  /** Returns a dataset in the catalog given its name. */
  def findDataset(name: Identifier): IO[Option[Dataset]] =
    datasets.find(_.id.forall(_ == name)).compile.last
}

object Catalog {

  /** A [[Catalog]] containing the given datasets. */
  def apply(ds: Dataset*): Catalog = new Catalog {
    override val datasets: Stream[IO, Dataset] = Stream.emits(ds)
  }

  /** A [[Catalog]] that contains no datasets. */
  def empty: Catalog = Catalog()

  implicit def monoid: Monoid[Catalog] = new Monoid[Catalog] {
    def empty: Catalog = Catalog.empty

    def combine(x: Catalog, y: Catalog): Catalog = new Catalog {
      override val datasets: Stream[IO, Dataset] = x.datasets ++ y.datasets

      override def findDataset(name: Identifier): IO[Option[Dataset]] =
        (OptionT(x.findDataset(name)) <+> OptionT(y.findDataset(name))).value
    }
  }
}
