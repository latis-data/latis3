package latis.server

import cats.effect.IO
import cats.effect.Resource
import com.comcast.ip4s._
import org.http4s.HttpRoutes
import org.http4s.Method
import org.http4s.implicits._
import org.http4s.server.Router
import org.http4s.server.Server
import org.http4s.ember.server._
import org.http4s.server.middleware.CORS
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

    EmberServerBuilder.default[IO]
      .withHost(host"0.0.0.0")
      .withPort(conf.port)
      .withHttpApp {
        LatisErrorHandler(
          LatisServiceLogger(
            Router(conf.mapping ->
              CORS.policy
                .withAllowOriginAll
                .withAllowMethodsIn(Set(Method.GET, Method.HEAD))
                .apply(constructRoutes(interfaces))
            ).orNotFound,
            logger
          ),
          logger
        )
      }
      .build
  }
}
