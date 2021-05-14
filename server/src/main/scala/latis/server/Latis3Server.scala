package latis.server

import scala.concurrent.ExecutionContext

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import org.http4s.HttpRoutes
import org.http4s.implicits._
import org.http4s.server.Router
import org.http4s.server.blaze._
import org.http4s.server.middleware.CORS
import org.http4s.server.middleware.CORS.DefaultCORSConfig
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._

import latis.catalog.FdmlCatalog
import cats.effect.Resource

object Latis3Server extends IOApp {

  private val loader: ServiceInterfaceLoader =
    new ServiceInterfaceLoader()

  private val latisConfigSource: ConfigSource =
    ConfigSource.default.at("latis")

  private def getCatalogConf: IO[CatalogConf] =
    latisConfigSource.at("fdml").loadF[IO, CatalogConf](blocker)

  private def getServerConf: IO[ServerConf] =
    latisConfigSource.loadF[IO, ServerConf](blocker)

  private def getServiceConf: IO[ServiceConf] =
    latisConfigSource.loadF[IO, ServiceConf](blocker)

  private def constructRoutes(
    services: List[(ServiceSpec, ServiceInterface)]
  ): HttpRoutes[IO] = {
    val routes: List[(String, HttpRoutes[IO])] = services.map {
      case (spec, service) => (spec.mapping, service.routes)
    }
    Router(routes:_*)
  }

  private def startServer(
    routes: HttpRoutes[IO],
    conf: ServerConf,
    logger: StructuredLogger[IO]
  ): IO[Unit] = conf match {
    case ServerConf(port, mapping) =>
      BlazeServerBuilder[IO](ExecutionContext.global)
        .bindHttp(port, "0.0.0.0")
        .withHttpApp {
          LatisServiceLogger(Router(mapping ->
            CORS(routes, DefaultCORSConfig)
          ).orNotFound, logger)
        }
        .withoutBanner
        .serve
        .compile
        .drain
  }

  def run(args: List[String]): IO[ExitCode] =
    Resource.unit[IO].use { blocker =>
      for {
        logger      <- Slf4jLogger.create[IO]
        catalogConf <- getCatalogConf(blocker)
        catalog      = FdmlCatalog.fromDirectory(
          catalogConf.dir,
          catalogConf.validate
        )
        serverConf  <- getServerConf(blocker)
        serviceConf <- getServiceConf(blocker)
        services    <- loader.loadServices(serviceConf, catalog)
        routes       = constructRoutes(services)
        _           <- startServer(routes, serverConf, logger)
      } yield ExitCode.Success
    }
}
