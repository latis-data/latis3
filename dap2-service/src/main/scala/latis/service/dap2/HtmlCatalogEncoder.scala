package latis.service.dap2

import cats.effect.*
import fs2.Stream
import scalatags.Text
import scalatags.Text.all.*
import scalatags.Text.tags2.details
import scalatags.Text.tags2.style
import scalatags.Text.tags2.summary

import latis.catalog.Catalog

object HtmlCatalogEncoder {

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
              "summary { width: 100%; }" +
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

  /** Provides an HTML table representation of a Catalog's datasets */
  private[dap2] def datasetTable(catalog: Catalog, prefix: String = ""): IO[Text.TypedTag[String]] =
    catalog.datasets.map { ds =>
      val id = ds.id.fold("")(_.asString)
      val title = ds.metadata.getProperty("title").getOrElse(id)
      // Build table row, preserve id for sorting
      (id, tr(
        td(id),
        td(a(href := prefix + id + ".meta")(title))
      ))
    }.compile.toList.map { catalogDatasets =>
      if (catalogDatasets.isEmpty) {
        div()
      } else {
        table(
          caption(i("Datasets")),
          tr(
            th("id"),
            th("title")
          ),
          catalogDatasets.sortBy(_._1).map(_._2)
        )
      }
    }

  /** Provides a recursive HTML table representation of a Catalog's sub-catalogs */
  private[dap2] def subcatalogTable(catalog: Catalog, prefix: String = ""): IO[Text.TypedTag[String]] = {
    val catalogs = Stream.emits(catalog.catalogs.toList)
    catalogs.evalMap { c =>
      val id = c._1.asString
      val cat = c._2
      for {
        datasets <- datasetTable(cat, prefix + id + "/")
        subcatalogs <- subcatalogTable(cat, prefix + id + "/")
      } yield {
        // Build table row, preserve id for sorting
        (id, tr(
          td(details(
            summary(b(i(u(id))), emptyTab,
              a(href := prefix + id + "/")(forwardArrow)
            ),
            div(datasets,
              subcatalogs)
          ))
        ))
      }
    }.compile.toList.map { catalogSubcatalogs =>
      if (catalogSubcatalogs.isEmpty) {
        div()
      } else {
        table(`class` := "subcatalog",
          caption(i("Subcatalogs")),
          catalogSubcatalogs.sortBy(_._1).map(_._2)
        )
      }
    }
  }
}
