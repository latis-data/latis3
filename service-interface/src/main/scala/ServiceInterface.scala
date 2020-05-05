package latis.server

import cats.effect.IO
import org.http4s.HttpRoutes

import latis.input.FdmlDatasetResolver

abstract class ServiceInterface(resolver: FdmlDatasetResolver) {

  def routes: HttpRoutes[IO]
}
