package latis.service.dap2

import cats.effect._
import scalatags.Text
import scalatags.Text.all._

import latis.catalog.Catalog

object HtmlCatalogEncoder {

  /** Provides an HTML representation of a Catalog. */
  def encode(catalog: Catalog): IO[Text.TypedTag[String]] =
    catalogTable(catalog).map { table =>
      html(
        body(
          h1("LaTiS 3 DAP2 Server"),
          hr(),
          table
        )
      )
    }

  /** Provides an HTML table representation of a Catalog. */
  private[dap2] def catalogTable(catalog: Catalog): IO[Text.TypedTag[String]] =
    catalog.datasets.map { ds =>
      val id = ds.id.fold("")(_.asString)
      val title = ds.metadata.getProperty("title").getOrElse(id)
      tr(
        td(id),
        td(a(href := id+".meta")(title))
      )
    }.compile.toList.map { catalogEntries =>
      table(
        caption(b(i(u("Catalog")))),
        tr(
          th("id"),
          th("title"),
        ),
        catalogEntries
      )
    }
}
