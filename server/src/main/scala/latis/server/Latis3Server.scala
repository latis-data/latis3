package latis.server

import scala.concurrent.ExecutionContext

import cats.effect.Blocker
import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import org.http4s.HttpRoutes
import org.http4s.implicits._
import org.http4s.server.Router
import org.http4s.server.blaze._
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._

object Latis3Server extends IOApp {

  private val loader: ServiceInterfaceLoader =
    new ServiceInterfaceLoader()

  private val latisConfigSource: ConfigSource =
    ConfigSource.default.at("latis")

  private def getServerConf(blocker: Blocker): IO[ServerConf] =
    latisConfigSource.loadF[IO, ServerConf](blocker)

  private def getServiceConf(blocker: Blocker): IO[ServiceConf] =
    latisConfigSource.loadF[IO, ServiceConf](blocker)

  private def constructRoutes(
    services: List[(ServiceSpec, ServiceInterface)]
  ): HttpRoutes[IO] = {
    val routes: List[(String, HttpRoutes[IO])] = services.map {
      case (spec, service) => (spec.mapping, service.routes)
    }
    Router(routes:_*)
  }

  private def startServer(routes: HttpRoutes[IO], conf: ServerConf): IO[Unit] =
    conf match {
      case ServerConf(port, mapping) =>
        BlazeServerBuilder[IO](ExecutionContext.global)
          .bindHttp(port, "0.0.0.0")
          .withHttpApp {
            Router(mapping -> routes).orNotFound
          }
          .serve
          .compile
          .drain
    }

  def run(args: List[String]): IO[ExitCode] =
    Blocker[IO].use { blocker =>
      for {
        serverConf  <- getServerConf(blocker)
        serviceConf <- getServiceConf(blocker)
        services    <- loader.loadServices(serviceConf)
        routes       = constructRoutes(services)
        _           <- startServer(routes, serverConf)
      } yield ExitCode.Success
    }
}
