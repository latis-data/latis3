package latis.server

import scala.concurrent.ExecutionContext

import cats.effect.IO
import cats.effect.Resource
import org.http4s.HttpRoutes
import org.http4s.implicits._
import org.http4s.server.Router
import org.http4s.server.Server
import org.http4s.blaze.server._
import org.http4s.server.middleware.CORS
import org.http4s.server.middleware.CORSConfig
import org.typelevel.log4cats.StructuredLogger
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import cats.effect.Temporal

object Latis3ServerBuilder {

  val latisConfigSource: ConfigSource =
    ConfigSource.default.at("latis")

  val getCatalogConf: IO[CatalogConf] =
    latisConfigSource.at("fdml").loadF[IO, CatalogConf]()

  val getServerConf: IO[ServerConf] =
    latisConfigSource.loadF[IO, ServerConf]()

  def mkServer(
    conf: ServerConf,
    interfaces: List[(String, ServiceInterface)],
    logger: StructuredLogger[IO],
    executionContext: ExecutionContext = ExecutionContext.global
  )(
    implicit timer: Temporal[IO]
  ): Resource[IO, Server] = {

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
            CORSConfig.default
          )).orNotFound,
          logger
        )
      }
      .withoutBanner
      .resource
  }
}
