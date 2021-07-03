package latis.server

import scala.annotation.unused

import cats.effect.IO
import org.http4s.HttpRoutes

import latis.catalog.Catalog

abstract class ServiceInterface(@unused catalog: Catalog) {

  def routes: HttpRoutes[IO]
}
