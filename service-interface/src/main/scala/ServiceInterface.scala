package latis.server

import cats.effect.IO
import org.http4s.HttpRoutes

import latis.catalog.Catalog

abstract class ServiceInterface(catalog: Catalog) {

  def routes: HttpRoutes[IO]
}
