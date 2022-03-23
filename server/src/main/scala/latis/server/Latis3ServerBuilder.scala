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
import pureconfig.module.ip4s._
import cats.effect.Temporal

import latis.service.landing.LandingPageService
import latis.util.ReflectionUtils.getClassByName

object Latis3ServerBuilder {

  val latisConfigSource: ConfigSource =
    ConfigSource.default.at("latis")

  val getCatalogConf: IO[CatalogConf] =
    latisConfigSource.at("fdml").loadF[IO, CatalogConf]()

  val getServerConf: IO[ServerConf] =
    latisConfigSource.loadF[IO, ServerConf]()

  def makeServiceInfo(className: String): ServiceInfo = {
    def exception2Option[T](function: String=>T, input: String): Option[T] = {
      try {
        Some(function(input))
      } catch {
        case _: Exception => None
      }
    }
    def getField(obj: Class[_], field: String): String = {
      obj.getDeclaredField(field).get(obj).asInstanceOf[String]
    }
    println(className)
    val info = exception2Option(getClassByName, className)
    println(info.get)
    println(info.get.getDeclaredFields.mkString("\n"))
    println(getField(info.get,"name"))
    val name = exception2Option(getField(info.get,_),"name")
    val version = exception2Option(getField(info.get,_),"version")
    val latisVersion = exception2Option(getField(info.get,_),"latisVersion")
    val buildTime = exception2Option(getField(info.get,_),"buildTime")
    ServiceInfo(name.getOrElse("LaTiS Server"), version, latisVersion, buildTime)
  }

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
        case (prefix, service) => (prefix, service.routes)
      } :+ ("/", new LandingPageService(makeServiceInfo("latis.util.BuildInfo$")).routes)
      Router(routes:_*)
    }

    EmberServerBuilder.default[IO]
      .withHost(host"0.0.0.0")
      .withPort(conf.port)
      .withHttpApp {
        LatisErrorHandler(
          LatisServiceLogger(
            Router(conf.prefix ->
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
