package latis.server

import cats.effect.IO
import cats.effect.Resource
import cats.effect.Temporal
import cats.syntax.all.*
import com.comcast.ip4s.*
import org.http4s.HttpRoutes
import org.http4s.Method
import org.http4s.ember.server.*
import org.http4s.implicits.*
import org.http4s.server.Router
import org.http4s.server.Server
import org.http4s.server.middleware.CORS
import org.typelevel.log4cats.StructuredLogger
import pureconfig.ConfigSource
import pureconfig.module.catseffect.syntax.*

import latis.service.landing.DefaultLandingPage
import latis.service.landing.LandingPage
import latis.util.ReflectionUtils.getClassByName

object Latis3ServerBuilder {

  val latisConfigSource: ConfigSource =
    ConfigSource.default.at("latis")

  val getCatalogConf: IO[CatalogConf] =
    latisConfigSource.at("fdml").loadF[IO, CatalogConf]()

  val getServerConf: IO[ServerConf] =
    latisConfigSource.loadF[IO, ServerConf]()

  private[server] def makeServiceInfo(className: String): ServiceInfo = {
    val classObj = Either.catchNonFatal(getClassByName(className)).toOption

    def getField(obj: Class[?], field: String): Option[String] = Either.catchNonFatal {
      val f = obj.getDeclaredField(field)
      f.setAccessible(true)
      f.get(obj).asInstanceOf[String]
    }.toOption

    classObj.map { clss =>
      val service = getField(clss, "service")
      val version = getField(clss, "version")
      val latisVersion = getField(clss, "latisVersion")
      val buildTime = getField(clss, "buildTime")

      ServiceInfo(service.getOrElse("LaTiS Server"), version, latisVersion, buildTime)
    }.getOrElse(ServiceInfo("LaTiS Server", None, None, None))
  }

  def defaultLandingPage: LandingPage = new DefaultLandingPage(makeServiceInfo("latis.util.BuildInfo$"))

  @annotation.targetName("mkServerOld")
  def mkServer(
    conf: ServerConf,
    landingPage: LandingPage,
    interfaces: List[(String, ServiceInterface)],
    logger: StructuredLogger[IO],
  )(
    implicit timer: Temporal[IO]
  ): Resource[IO, Server] = {
    val routes = interfaces.map { (name, si) => (name, si.routes) }
    mkServer(conf, landingPage, routes, logger)
  }

  def mkServer(
    conf: ServerConf,
    landingPage: LandingPage,
    interfaces: List[(String, HttpRoutes[IO])],
    logger: StructuredLogger[IO],
  )(
    implicit timer: Temporal[IO]
  ): Resource[IO, Server] = {

    val routes = interfaces :+ ("/", landingPage.routes)
    val router = Router(routes *)

    IO.local(Map.empty[String, String])
      .toResource
      .flatMap { implicit iol =>
        EmberServerBuilder.default[IO]
          .withHost(host"0.0.0.0")
          .withPort(conf.port)
          .withHttpApp {
            LatisServiceLogger(
              LatisErrorHandler(
                Router(conf.prefix ->
                  CORS.policy
                    .withAllowOriginAll
                    .withAllowMethodsIn(Set(Method.GET, Method.HEAD))
                    .apply(router)
                ).orNotFound,
                logger
              ),
              logger
            )
          }
          .build
      }
  }
}
