package latis.server

import scala.annotation.unused

import cats.effect.IO
import org.http4s.HttpRoutes

import latis.catalog.Catalog
import latis.ops.OperationRegistry

abstract class ServiceInterface(
  @unused catalog: Catalog,
  @unused operationRegistry: OperationRegistry
) {

  def routes: HttpRoutes[IO]
}
