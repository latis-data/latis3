package latis.server

import cats.effect.IO
import org.http4s.HttpRoutes

import latis.catalog.Catalog
import latis.ops.OperationRegistry

class TestServiceInterface(
  c: Catalog,
  r: OperationRegistry
) extends ServiceInterface(c, r) {
  def routes: HttpRoutes[IO] = HttpRoutes.empty
}

class ServiceInterfaceLoaderSuite extends munit.CatsEffectSuite {

  test("load a service interface") {
    ServiceInterfaceLoader().loadService(
      Thread.currentThread().getContextClassLoader(),
      "latis.server.TestServiceInterface",
      Catalog.empty,
      OperationRegistry.empty
    ).map { si =>
      assertEquals(si.getClass().getSimpleName(), "TestServiceInterface")
    }
  }
}
