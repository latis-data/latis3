package latis.service.dap2

import cats.effect._
import cats.effect.unsafe.implicits.global
import fs2.Stream
import scalatags.Text
import scalatags.Text.all._
import scalatags.Text.tags2.details
import scalatags.Text.tags2.summary

import latis.util.Identifier.IdentifierStringContext
import scalatags.Text.tags2.style

import latis.catalog.Catalog

object HtmlCatalogEncoder {

  private val emptyCell = raw("&#8212;")
  private val emptyTab = raw("&nbsp&nbsp&nbsp&nbsp")
  private val backArrow = raw("&#8689")
  private val forwardArrow = raw("&#8690")

  /** Provides a complete HTML representation of a Catalog. */
  def encode(catalog: Catalog): IO[Text.TypedTag[String]] = {
    for {
      dsTable <- datasetTable(catalog)
      scTable <- subcatalogTable(catalog)
    } yield {
      html(
        head(
          style(
            "table { height: 50px; padding: 15px 0; margin: 15px 0; border:2px solid; }" +
              "th, td { padding: 0 15px; text-align: left; }" +
              "caption { text-align: left; }" +
              ".subcatalog { width: 100%; }"
          )
        ),
        body(
          h1("LaTiS 3 DAP2 Server"),
          hr(),
          h2(b(i(u("Catalog"))), emptyTab, a(href := "../")(backArrow)),
          div(Text.all.style := "max-width: 50%;",
            dsTable,
            scTable
          ),
          hr()
        )
      )
    }
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
          th("title")
        ),
        catalogEntries
      )
    }

  /** Provides an HTML table representation of a Catalog's datasets */
  private[dap2] def datasetTable(catalog: Catalog, prefix: String = ""): IO[Text.TypedTag[String]] =
    catalog.datasets.map { ds =>
      val id = ds.id.fold("")(_.asString)
      val title = ds.metadata.getProperty("title").getOrElse(id)
      tr(
        td(id),
        td(a(href := prefix + id + ".meta")(title))
      )
    }.ifEmptyEmit(tr(td(emptyCell),td(emptyCell))).compile.toList.map { catalogDatasets =>
      table(
        caption(i("Datasets")),
        tr(
          th("id"),
          th("title")
        ),
        catalogDatasets
      )
    }

  /** Provides a recursive HTML table representation of a Catalog's sub-catalogs */
  private[dap2] def subcatalogTable(catalog: Catalog, prefix: String = ""): IO[Text.TypedTag[String]] = {
    val catalogs = Stream.eval(IO(catalog.catalogs.toList)).flatMap(Stream.emits(_))
    catalogs.evalMap { c =>
      val id = c._1.asString
      val cat = c._2
      for {
        datasets <- datasetTable(cat, prefix + id + "/")
        subcatalogs <- subcatalogTable(cat, prefix + id + "/")
      } yield {
        tr(
          td(details(
            summary(Text.all.style := "width:100%;",
              b(i(u(id))), emptyTab, a(href := prefix + id + "/")(forwardArrow)),
            div(datasets,
              subcatalogs)
          ))
        )
      }
    }.ifEmptyEmit(tr(td(hr()))).compile.toList.map { tab =>
      table(`class` := "subcatalog",
        caption(i("Subcatalogs")),
        tab
      )
    }
  }
}
