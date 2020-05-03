package latis.server

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

  private val latisConfigSource: ConfigSource =
    ConfigSource.default.at("latis")

  private val getDependencyConf: IO[DependencyConf] =
    latisConfigSource.loadF[IO, DependencyConf]

  private val getRepositoryConf: IO[RepositoryConf] =
    latisConfigSource.loadF[IO, RepositoryConf]

  private val getServerConf: IO[ServerConf] =
    latisConfigSource.loadF[IO, ServerConf]

  private val getServiceConf: IO[ServiceConf] =
    latisConfigSource.loadF[IO, ServiceConf]

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
        BlazeServerBuilder[IO]
          .bindHttp(port, "0.0.0.0")
          .withHttpApp {
            Router(mapping -> routes).orNotFound
          }
          .serve
          .compile
          .drain
    }

  def run(args: List[String]): IO[ExitCode] =
    for {
      serverConf     <- getServerConf
      serviceConf    <- getServiceConf
      repositoryConf <- getRepositoryConf
      dependencyConf <- getDependencyConf
      loader         <- new DependencyFetcher(
        repositoryConf,
        dependencyConf
      ).fetch
      services       <- new ServiceInterfaceLoader(
        serviceConf,
        loader
      ).loadServices
      routes          = constructRoutes(services)
      _              <- startServer(routes, serverConf)
    } yield ExitCode.Success
}
