package latis.catalog

import java.time.LocalDate

import cats.effect.IO
import fs2.Stream

import latis.dataset.Dataset
import latis.util.Bounds
import latis.util.Identifier

/**
 * Experimental alternate Catalog implementation
 *
 * This enables the capture of catalog metadata.
 * This catalog implements the existing Catalog so we can ease
 * the transition to use this approach instead.
 * 
 * @param id catalog identifier
 * @param title optional short name
 * @param description optional description
 * @param timeBounds optional temporal coverage in terms of [[LocalDate]]
 * @param catalog effectful list of sub-catalogs
 * @param dataset effectful list of datasets in this catalog
 */
case class Catalog2(
  id: Identifier,
  title: Option[String] = None,
  description: Option[String] = None,
  timeBounds: Option[Bounds[LocalDate]] = None,
  catalog: IO[List[Catalog2]] = IO(List.empty),
  dataset: IO[List[Dataset]] = IO(List.empty)
) extends Catalog {

  override def datasets: Stream[IO, Dataset] = Stream.evals(dataset)

  override val catalogs: IO[Map[Identifier, Catalog2]] =
    catalog.map(_.map(c => (c.id, c)).toMap)
}
