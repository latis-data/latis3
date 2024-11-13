package latis.service.dap2

import cats.data.NonEmptyList
import cats.effect.*
import cats.syntax.all.*
import io.circe.Json
import io.circe.syntax.*

import latis.catalog.Catalog
import latis.dataset.Dataset
import latis.util.Identifier

object JsonCatalogEncoder {

  /** Provides a JSON representation of a Catalog for a dap2 response. */
  def encode(catalog: Catalog, id: Option[Identifier] = None): IO[Json] =
    for {
      subs <- catalog.catalogs
      cats <- subs.toList.traverse { case (id, cat) => encode(cat, Some(id)) }
      dss  <- catalog.datasets.compile.toList.map(dss => dss.map(datasetToJson))
    } yield {
      val fields = List(
        id.map(id => "identifier" -> id.asString.asJson),
        NonEmptyList.fromList(cats).map(cats => "catalog" -> cats.asJson),
        NonEmptyList.fromList(dss).map(dss => "dataset" -> dss.asJson)
      ).unite //keep only fields that are defined
      Json.obj(fields *)
    }

  /** Provides a JSON representation of a Dataset in a catalog. */
  private def datasetToJson(dataset: Dataset): Json = {
    val id = dataset.id.map(_.asString).getOrElse("unknown")
    Json.obj(
      "identifier" -> id.asJson,
      "title"      -> dataset.metadata.getProperty("title", id).asJson
    )
  }

}
