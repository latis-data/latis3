package latis.server

import scala.concurrent.ExecutionContext

import cats.effect.Blocker
import cats.effect.ContextShift
import cats.effect.IO
import cats.effect.Resource
import cats.effect.Timer
import org.http4s.HttpRoutes
import org.http4s.implicits._
import org.http4s.server.Router
import org.http4s.server.Server
import org.http4s.server.blaze._
import org.http4s.server.middleware.CORS
import org.http4s.server.middleware.CORS.DefaultCORSConfig
import org.typelevel.log4cats.StructuredLogger
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._

object Latis3ServerBuilder {

  val latisConfigSource: ConfigSource =
    ConfigSource.default.at("latis")

  def getCatalogConf(blocker: Blocker)(implicit cs: ContextShift[IO]): IO[CatalogConf] =
    latisConfigSource.at("fdml").loadF[IO, CatalogConf](blocker)

  def getServerConf(blocker: Blocker)(implicit cs: ContextShift[IO]): IO[ServerConf] =
    latisConfigSource.loadF[IO, ServerConf](blocker)

  def mkServer(
    conf: ServerConf,
    interfaces: List[(String, ServiceInterface)],
    logger: StructuredLogger[IO],
    executionContext: ExecutionContext = ExecutionContext.global
  )(
    implicit cs: ContextShift[IO],
    timer: Timer[IO]
  ): Resource[IO, Server[IO]] = {

    def constructRoutes(
      interfaces: List[(String, ServiceInterface)]
    ): HttpRoutes[IO] = {
      val routes = interfaces.map {
        case (mapping, service) => (mapping, service.routes)
      }
      Router(routes:_*)
    }

    BlazeServerBuilder[IO](executionContext)
      .bindHttp(conf.port, "0.0.0.0")
      .withHttpApp {
        LatisServiceLogger(
          Router(conf.mapping -> CORS(
            constructRoutes(interfaces),
            DefaultCORSConfig
          )).orNotFound,
          logger
        )
      }
      .withoutBanner
      .resource
  }
}
