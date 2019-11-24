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
import pureconfig.module.catseffect._

object Latis3Server extends IOApp {

  val loader: ServiceInterfaceLoader =
    new ServiceInterfaceLoader()

  private val latisConfigSource: ConfigSource =
    ConfigSource.default.at("latis")

  val getServerConf: IO[ServerConf] =
    latisConfigSource.loadF[IO, ServerConf]

  val getServiceConf: IO[ServiceConf] =
    latisConfigSource.loadF[IO, ServiceConf]

  def constructRoutes(services: List[(ServiceSpec, ServiceInterface)]): HttpRoutes[IO] = {
    val routes: List[(String, HttpRoutes[IO])] = services.map {
      case (ServiceSpec(_, _, mapping, _), service) => (mapping, service.routes)
    }
    Router(routes:_*)
  }

  def startServer(routes: HttpRoutes[IO], conf: ServerConf): IO[Unit] =
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
      serverConf  <- getServerConf
      serviceConf <- getServiceConf
      services    <- loader.loadServices(serviceConf)
      routes       = constructRoutes(services)
      _           <- startServer(routes, serverConf)
    } yield ExitCode.Success
}
