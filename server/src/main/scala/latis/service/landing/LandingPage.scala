package latis.service.landing

import cats.effect.IO
import org.http4s.HttpRoutes

trait LandingPage {

  def routes: HttpRoutes[IO]
}
