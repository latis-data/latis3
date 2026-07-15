package latis.service.dap2

import cats.data.NonEmptyList
import cats.effect.*
import cats.syntax.all.*
import io.circe.Json
import io.circe.syntax.*

import latis.catalog.Catalog
import latis.dataset.Dataset
import latis.util.Identifier
import latis.util.Identifier.*

object JsonCatalogEncoder {

  /**
   * Provides a JSON representation of a Catalog for a dap2 response.
   *
   * This method may be called recursively as the catalog is traversed.
   * An optional depth will limit how far the recursion will proceed.
   * A depth of 0 will result in an empty JSON object.
   *
   * @param catalog The Catalog to be encoded
   * @param id Optional identifier fot the given catalog
   */
  def encode(
    catalog: Catalog,
    id: Option[Identifier] = None,
    depth: Option[Int] = None
  ): IO[Json] = {
    if (depth.contains(0)) {
      val json = id.map(id => Json.obj("identifier" -> id.asString.asJson))
        .getOrElse(Json.obj())
      IO.pure(json)
    } else for {
      subs <- catalog.catalogs
      cats <- subs.toList.sortBy(_._1).traverse {
        case (id, cat) => encode(cat, Some(id), depth.map(_ - 1))
      }
      dss  <- catalog.datasets.compile.toList.map { dss =>
        // NOTE: Datasets are expected to have an identifier. Getting
        // the identifier using the 'id' method is slow because we
        // check that they are valid. Here we assume they are valid
        // and just sort by the stored strings.
        dss.sortBy(_.metadata.unsafeGet("id")).map(datasetToJson)
      }
    } yield {
      val fields = List(
        id.map(id => "identifier" -> id.asString.asJson),
        NonEmptyList.fromList(cats).map(cats => "catalog" -> cats.asJson),
        NonEmptyList.fromList(dss).map(dss => "dataset" -> dss.asJson)
      ).unite //keep only fields that are defined
      Json.obj(fields *)
    }
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
