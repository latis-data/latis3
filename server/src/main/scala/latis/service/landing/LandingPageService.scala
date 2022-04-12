package latis.service.landing

import cats.effect.IO
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.scalatags.scalatagsEncoder
import scalatags.Text
import scalatags.Text.all._

import latis.server.ServiceInfo

class LandingPageService(serviceInfo: ServiceInfo) extends Http4sDsl[IO]{

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
