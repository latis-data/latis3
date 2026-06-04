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
      cats <- subs.toList.sortBy(_._1).traverse { 
        case (id, cat) => encode(cat, Some(id)) 
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
