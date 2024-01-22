package latis.service.landing

import cats.effect.IO
import org.http4s.dsl.io.*
import org.http4s.HttpRoutes
import org.http4s.scalatags.scalatagsEncoder
import scalatags.Text
import scalatags.Text.all.*

import latis.server.ServiceInfo

class DefaultLandingPage(serviceInfo: ServiceInfo) extends LandingPage {

  private val properties = List(
    "Version:" -> serviceInfo.version,
    "LaTiS Version:" -> serviceInfo.latisVersion,
    "Build Time:" -> serviceInfo.buildTime
  )
  private val propsTable = table(
    properties.filterNot(p => p._2.isEmpty).map { p =>
      tr(
        td(p._1),
        td(p._2)
      )
    }
  )
  private val landingPage: Text.TypedTag[String] =
      html(
        body(
          h1(serviceInfo.service),
          hr(),
          propsTable
        )
      )

  def routes: HttpRoutes[IO] =
    HttpRoutes.of {
      case GET -> Root =>
        Ok(landingPage)
    }
}
