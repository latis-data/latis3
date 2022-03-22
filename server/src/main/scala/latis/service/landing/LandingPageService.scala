package latis.service.landing

import cats.effect.IO
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.scalatags.scalatagsEncoder
import scalatags.Text
import scalatags.Text.all._

import latis.server.ServiceInfo

class LandingPageService(serviceInfo: ServiceInfo) extends Http4sDsl[IO]{

  private val landingPage: Text.TypedTag[String] =
      html(
        body(
          h1(serviceInfo.name),
          hr(),
          table(
            tr(
              td("Version:"),
              td(serviceInfo.version)
            ),
            tr(
              td("LaTiS Version:"),
              td(serviceInfo.latisVersion)
            ),
            tr(
              td("Build Time:"),
              td(serviceInfo.buildTime)
            )
          )
        )
      )

  def routes: HttpRoutes[IO] =
    HttpRoutes.of {
      case GET -> Root =>
        Ok(landingPage)
    }
}
