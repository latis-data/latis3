package latis.catalog

import cats.Foldable
import cats.Monoid
import cats.data.OptionT
import cats.effect.IO
import cats.syntax.all._
import fs2.Stream

import latis.dataset.Dataset
import latis.util.Identifier

/**
 * A collection of datasets and other catalogs.
 *
 * A catalog can list the datasets it contains and find subcatalogs
 * and datasets (in the root catalog and subcatalogs) by their
 * identifiers.
 *
 * Subcatalogs introduce a new scope for dataset identifiers:
 *
 * {{{
 * // All datasets in c1 are accessible using their usual IDs.
 * val c1 = Catalog(...)
 *
 * // All datasets in c1 are accessible in c2, but their IDs must now
 * // be qualified with 'a'.
 * val c2 = Catalog(...).addCatalog(id"a", c1)
 * }}}
 */
trait Catalog { self =>

  /** Returns all datasets in this catalog. */
  def datasets: Stream[IO, Dataset]

  /** This catalog's subcatalogs. */
  def catalogs: Map[Identifier, Catalog] = Map.empty

  /** Adds a single subcatalog to this catalog. */
  def addCatalog(id: Identifier, catalog: Catalog): Catalog = new Catalog {
    override val datasets: Stream[IO, Dataset] = self.datasets

    override val catalogs: Map[Identifier, Catalog] =
      self.catalogs + (id -> catalog)
  }

  /**
   * Returns a catalog containing only the datasets for which the
   * predicate is true.
   */
  def filter(p: Dataset => Boolean): Catalog = new Catalog {
    override val datasets: Stream[IO, Dataset] = self.datasets.filter(p)

    override val catalogs: Map[Identifier, Catalog] = self.catalogs.map {
      case (id, cat) => id -> cat.filter(p)
    }
  }

  /** Returns a subcatalog given its name. */
  def findCatalog(name: Identifier): Option[Catalog] =
    splitId(name) match {
      case (Nil, id)     => catalogs.get(id)
      case (q :: qs, id) =>
        catalogs.get(q).flatMap(_.findCatalog(concatId(qs, id)))
    }

  /**
   * Returns a dataset in this catalog or a subcatalog given its name.
   */
  def findDataset(name: Identifier): IO[Option[Dataset]] =
    splitId(name) match {
      case (Nil, id)     =>
        datasets.find(_.id.forall(_ == id)).compile.last
      case (q :: qs, id) =>
        catalogs.get(q).flatTraverse(_.findDataset(concatId(qs, id)))
    }

  /** Sets this catalog's subcatalogs. */
  def withCatalogs(scs: (Identifier, Catalog)*): Catalog = new Catalog {
    override val datasets: Stream[IO,Dataset] = self.datasets

    override val catalogs: Map[Identifier, Catalog] = scs.toMap
  }

  // Constructs a qualified ID.
  private def concatId(qs: List[Identifier], id: Identifier): Identifier = {
    val qual = qs.map(_.asString)
    if (qual.nonEmpty) {
      Identifier.fromString(s"""${qual.mkString(".")}.${id.asString}""").get
    } else {
      id
    }
  }

  // Splits an unofficially qualified identifier into a qualification
  // and an identifier.
  private def splitId(qid: Identifier): (List[Identifier], Identifier) = {
    val sections = qid.asString.split('.').toList
    if (sections.length == 1) {
      val qual = List.empty
      val id = Identifier.fromString(sections.head).get
      (qual, id)
    } else if (sections.length > 1) {
      val qual = sections.init.traverse(Identifier.fromString).get
      val id = Identifier.fromString(sections.last).get
      (qual, id)
    } else {
      // Should never happen. Identifiers cannot be empty strings, and
      // splitting on a non-empty string will return the string itself
      // if the delimiter is not in the string.
      throw new RuntimeException("Bug in qualified identifier splitting")
    }
  }
}

object Catalog {

  /** A [[Catalog]] containing the given datasets. */
  def apply(ds: Dataset*): Catalog = new Catalog {
    override val datasets: Stream[IO, Dataset] = Stream.emits(ds)
  }

  /** A [[Catalog]] that contains no datasets. */
  def empty: Catalog = Catalog()

  def fromFoldable[F[_]: Foldable](dss: F[Dataset]): Catalog =
    new Catalog {
      override val datasets: Stream[IO, Dataset] = Stream.foldable(dss)
    }

  implicit def monoid: Monoid[Catalog] = new Monoid[Catalog] {
    def empty: Catalog = Catalog.empty

    def combine(x: Catalog, y: Catalog): Catalog = new Catalog {
      override val datasets: Stream[IO, Dataset] = x.datasets ++ y.datasets

      override def findDataset(name: Identifier): IO[Option[Dataset]] =
        (OptionT(x.findDataset(name)) <+> OptionT(y.findDataset(name))).value
    }
  }
}
