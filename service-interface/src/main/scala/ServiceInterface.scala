package latis.server

import cats.effect.IO
import org.http4s.HttpRoutes

trait ServiceInterface {

  def routes: HttpRoutes[IO]
}
