package latis.service.dap2

import cats.data.NonEmptyList
import cats.effect.*
import cats.syntax.all.*
import io.circe.Json
import io.circe.syntax.*

import latis.catalog.Catalog
import latis.catalog.Catalog2
import latis.dataset.Dataset
import latis.util.*
import latis.util.Identifier.*


object JsonCatalogEncoder {

  /**
   * Provides a JSON representation of a Catalog for a dap2 response.
   *
   * This method may be called recursively as the catalog is traversed.
   * An optional depth will limit how far the recursion will proceed.
   * A depth of 0 will result in an empty JSON object.
   *
   * This will include properties of the experimental Catalog2.
   *
   * @param catalog The Catalog to be encoded
   * @param id Optional identifier fot the given catalog
   */
  def encode(
    catalog: Catalog,
    id: Option[Identifier] = None,
    depth: Option[Int] = None
  ): IO[Json] = {
    val props = {
      val identifier = id.map(id => ("identifier" -> id.asString.asJson))
      catalog match {
        case cat: Catalog2 => identifier.toList ++ catalogProperties(cat)
        case cat: Catalog  => identifier.toList
      }
    }
    if (depth.contains(0)) {
      // Just the catalog metadata with no subcatalogs or datasets
      Json.obj(props *).pure
    } else for {
      subs <- catalog.catalogs
      cats <- subs.toList.sortBy(_._1).traverse {
        case (id, cat) => encode(cat, id.some, depth.map(_ - 1))
      }
      dss  <- catalog.datasets.compile.toList.map { dss =>
        // NOTE: Datasets are expected to have an identifier. Getting
        // the identifier using the 'id' method is slow because we
        // check that they are valid. Here we assume they are valid
        // and just sort by the stored strings.
        dss.sortBy(_.metadata.unsafeGet("id")).map(datasetToJson)
      }
    } yield {
      // Add subcatalogs and datasets to the catalog properties
      val fields = props ++ List(
        NonEmptyList.fromList(cats).map(cats => "catalog" -> cats.asJson),
        NonEmptyList.fromList(dss).map(dss => "dataset" -> dss.asJson)
      ).unite
      Json.obj(fields *)
    }
  }

  /** Extra properties from a Catalog2 */
  private def catalogProperties(cat: Catalog2): List[(String, Json)] = {
    List(
      cat.title.map("title" -> _.asJson),
      cat.description.map("description" -> _.asJson),
      cat.timeBounds.flatMap {
        // Note, LocalDate.toString will use yyyy-MM-dd format as documented.
        // Catalog time coverage metadata should be interpreted as closed,
        // but for completeness...
        case PointBounds(v) => s"$v".some
        case NonEmptyBounds(ClosedBound(l), ClosedBound(u)) => s"$l/$u".some
        case NonEmptyBounds(ClosedBound(l), OpenBound(u)) => s"$l/$u".some
        case NonEmptyBounds(ClosedBound(l), InfiniteBound()) => s"$l/".some
        case NonEmptyBounds(OpenBound(l), ClosedBound(u)) => s"$l/$u".some
        case NonEmptyBounds(OpenBound(l), OpenBound(u)) => s"$l/$u".some
        case NonEmptyBounds(OpenBound(l), InfiniteBound()) => s"$l/".some
        case NonEmptyBounds(InfiniteBound(), ClosedBound(u)) => s"/$u".some
        case NonEmptyBounds(InfiniteBound(), OpenBound(u)) => s"/$u".some
        case NonEmptyBounds(InfiniteBound(), InfiniteBound()) => None
        case EmptyBounds() => None
      }.map("temporal" -> _.asJson)
    ).unite //keep only fields that are defined
  }

  /** Provides a JSON representation of a Dataset in a catalog. */
  private def datasetToJson(dataset: Dataset): Json = {
    // NOTE: Getting identifier via metadata to avoid slow valid
    // identifier check.
    val id = dataset.metadata.unsafeGet("id")
    Json.obj(
      "identifier" -> id.asJson,
      "title"      -> dataset.metadata.getProperty("title", id).asJson
    )
  }

}
