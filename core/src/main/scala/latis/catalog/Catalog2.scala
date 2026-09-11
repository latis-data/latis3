package latis.catalog

import cats.effect.IO
import fs2.Stream

import latis.dataset.Dataset
import latis.util.Identifier

/**
 * Experimental alternate Catalog implementation
 *
 * This enables the capture of catalog metadata.
 * This catalog implements the existing Catalog so we can ease
 * the transition to use this approach instead.
 */
case class Catalog2(
  id: Identifier,
  title: Option[String] = None,
  description: Option[String] = None,
  catalog: IO[List[Catalog2]] = IO(List.empty), //TODO: rename "catalogs"
  dataset: IO[List[Dataset]] = IO(List.empty) //TODO: rename "datasets"
) extends Catalog {
  //TODO: add Map for other properties?

  override def datasets: Stream[IO, Dataset] = Stream.evals(dataset)

  override val catalogs: IO[Map[Identifier, Catalog2]] =
    catalog.map(_.map(c => (c.id, c)).toMap)
}
